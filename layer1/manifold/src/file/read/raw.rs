use std::{cell, ops, os::fd::AsFd as _, pin};

use dope_core::{
    driver::{
        result::SubmitError,
        retained::flight,
        route::{self, Identity, indices},
        schedule::{self, ready::wake},
    },
    io::{self, event::Kind, fs, transfer},
};
use dope_runtime::executor;
use o3::buffer::bytes::bounded;

use crate::{
    app::{dispatch, dispatch::event},
    file::read::{self, Active, Error, Outcome, Phase, Reader, Request},
};

pub(super) struct Entry<'d> {
    pub(super) index: indices::SlotIndex,
    pub(super) phase: cell::Cell<Option<Phase<'d>>>,
    pub(super) waiter: cell::Cell<Option<wake::Waker<'d>>>,
    pub(super) detached: cell::Cell<bool>,
    request: cell::UnsafeCell<Option<Request<'d>>>,
}

impl<'d> Entry<'d> {
    pub(super) fn new(index: indices::SlotIndex) -> Self {
        Self {
            index,
            phase: cell::Cell::new(None),
            waiter: cell::Cell::new(None),
            detached: cell::Cell::new(false),
            request: cell::UnsafeCell::new(None),
        }
    }

    pub(super) fn insert(&self, request: Request<'d>) {
        // SAFETY: only a removed FREE membership admits insertion. No active
        // flight or ticket can name this vacant slot until publication completes.
        unsafe { *self.request.get() = Some(request) };
    }

    pub(super) fn take(&self) -> Request<'d> {
        // SAFETY: callers own a terminal/unsubmitted phase or Finish's native
        // quiescence proof. Native users and request borrows have ended.
        let Some(request) = (unsafe { &mut *self.request.get() }).take() else {
            unreachable!("request")
        };
        request
    }

    pub(super) fn with_request<T>(&self, run: impl FnOnce(&mut Request<'d>) -> T) -> T {
        // SAFETY: private callers first take this slot's phase. They invoke no
        // application code and finish the loan before restoring or freeing it.
        // In-flight reads are borrowed only after their matching terminal CQ.
        let Some(request) = (unsafe { &mut *self.request.get() }).as_mut() else {
            unreachable!("request")
        };
        run(request)
    }
}

pub(super) fn submit<'d, ID: Identity>(
    entry: &Entry<'d>,
    slots: &flight::Slots<'d, ID>,
    driver: &mut dispatch::raw::Context<'_, '_, 'd, dispatch::raw::Retained>,
) -> Result<Active<'d>, SubmitError> {
    let target = driver
        .driver()
        .driver_ref()
        .targets::<ID>()
        .bind(entry.index, route::Epoch::INITIAL);
    entry.with_request(|request| {
        match request {
            Request::Open(request) => {
                // SAFETY: the fixed slot never moves this request's path or inline
                // native options until its original completion or native quiescence.
                unsafe {
                    driver
                        .reborrow()
                        .submit_file(slots, request.submission(target))
                }
                .map(Active::Open)
            }
            Request::Read(request) => {
                // SAFETY: Read retains a nonempty bounded extent, and neither the
                // initialized allocation nor its extent changes while it is submitted.
                let output = unsafe { o3::buffer::raw::Project::as_inner_mut(&mut request.output) };
                let output = bounded::Value::from_raw(unsafe {
                    bounded::raw::Input::new(output.as_mut_slice())
                });
                let operation =
                    fs::Submission::read_at(request.file.as_fd(), output, request.offset, target);
                // SAFETY: this installed owner retains the exact FD and exclusive
                // stable heap through the original completion or native quiescence.
                unsafe { driver.reborrow().submit_file(slots, operation) }.map(Active::Read)
            }
        }
    })
}

// SAFETY: installation retains the port's fixed request slots and the flight
// group; neither cancellation nor shutdown releases submitted resources.
unsafe impl<'d, ID: Identity> dispatch::raw::Manifold<'d> for Reader<'_, 'd, ID> {
    type Route = ID;
    type Dispatch = event::Driven<dispatch::raw::Retained>;
    type Install = dispatch::raw::Plain;
    type Finish = Self;
    type Activate = dispatch::raw::Omit;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Plain;
    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self.as_mut().get_mut().advance(turn, driver);
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self.as_mut().get_mut().close();
        self
    }
}

// SAFETY: unit selection observes this exact installed Reader.
unsafe impl<'d, ID: Identity> dispatch::raw::Probe<'d, Self> for Reader<'_, 'd, ID> {
    type Plan = ();
    fn select() -> Option<()> {
        Some(())
    }
    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> schedule::Progress<'d> {
        let this = call.owner().get_ref();
        if !this.port.slots.is_empty::<{ read::WORK }>() || this.closing.is_some() {
            schedule::Progress::Runnable
        } else if this.port.pending.get() == 0 {
            schedule::Progress::Quiescent
        } else {
            schedule::Progress::waiting(call.region())
        }
    }
}

// SAFETY: the view only borrows this installed owner's request port. Admission
// cannot access a submitted buffer or release its descriptor.
unsafe impl<'d, ID: Identity> dispatch::raw::Control<'d> for Reader<'_, 'd, ID> {
    type View<'step>
        = &'step read::Port<'d>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: dispatch::raw::Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        owner.as_ref().get_ref().port
    }
}

impl<'d, ID: Identity> event::Drive<'d> for Reader<'_, 'd, ID> {
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
        let (app, (), (event, _)) = call.into_parts();
        let this = app.as_ref().get_ref();
        match event.into_kind() {
            Kind::Read(done) => {
                let Some(slot) = this
                    .port
                    .slots
                    .get(done.token().slot().index().into_usize())
                else {
                    return ops::ControlFlow::Break(Kind::Read(done).into());
                };
                let entry = slot.state();
                match entry.phase.take() {
                    Some(Phase::Active {
                        flight: Active::Read(flight),
                        ..
                    }) if flight.matches(done.token()) => {
                        drop(flight);
                        let result = entry.with_request(|request| {
                            let Request::Read(request) = request else {
                                unreachable!("operation")
                            };
                            match done.outcome() {
                                Ok(observed)
                                    if observed.is_greater(transfer::Len::from_nonzero(
                                        request.length,
                                    )) =>
                                {
                                    Err(Error::Amount {
                                        observed,
                                        requested: request.length,
                                    })
                                }
                                Ok(amount) => Ok(amount),
                                Err(cause) => Err(Error::System(cause)),
                            }
                        });
                        this.port.settle(&slot, Outcome::Read(result));
                    }
                    phase => {
                        entry.phase.set(phase);
                        return ops::ControlFlow::Break(Kind::Read(done).into());
                    }
                }
            }
            Kind::Open(done) => {
                let Some(slot) = this
                    .port
                    .slots
                    .get(done.token().slot().index().into_usize())
                else {
                    return ops::ControlFlow::Break(Kind::Open(done).into());
                };
                let entry = slot.state();
                match entry.phase.take() {
                    Some(Phase::Active {
                        flight: Active::Open(flight),
                        ..
                    }) if flight.matches(done.token()) => {
                        drop(flight);
                        let result = match done.into_parts().1 {
                            io::event::file::open::Outcome::Opened(fd) => Ok(fs::File::from(fd)),
                            io::event::file::open::Outcome::Failed(cause) => {
                                Err(Error::System(cause))
                            }
                        };
                        this.port.settle(&slot, Outcome::Open(result));
                    }
                    phase => {
                        entry.phase.set(phase);
                        return ops::ControlFlow::Break(Kind::Open(done).into());
                    }
                }
            }
            other => return ops::ControlFlow::Break(other.into()),
        }
        ops::ControlFlow::Continue(())
    }
}
