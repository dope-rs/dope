use std::{cell, mem, ops, os::fd::AsFd as _, pin};

use dope_core::{
    driver::{
        retained::flight,
        route::{self, Identity, indices},
        schedule::{self, ready::wake},
    },
    io::{self, event::Kind, fs, transfer},
};
use dope_runtime::executor;
use o3::{
    alloc,
    buffer::bytes::bounded,
    mem::{Rejected, Return},
};

use crate::{
    app::{dispatch, dispatch::event},
    file::read::{
        self, Active, Error, Metadata, MetadataError, MetadataState, Outcome, Phase, Reader,
        Request,
    },
};

pub(super) struct Scratch {
    output: fs::metadata::Output,
    pub(super) next: Option<Box<Self>>,
}

impl Scratch {
    fn acquire(free: &mut Option<Box<Self>>) -> Result<Box<Self>, Error> {
        match free.take() {
            Some(mut scratch) => {
                *free = scratch.next.take();
                Ok(scratch)
            }
            None => alloc::BoxExt::try_new(Self {
                output: fs::metadata::Output::UNINIT,
                next: None,
            })
            .map_err(Error::Allocation),
        }
    }

    pub(super) fn release(free: &mut Option<Box<Self>>, mut scratch: Box<Self>) {
        scratch.next = free.take();
        *free = Some(scratch);
    }
}

impl Metadata {
    fn complete(
        self,
        native: Result<(), dope_core::Errno>,
        free: &mut Option<Box<Scratch>>,
    ) -> (fs::File, Result<fs::metadata::Value, MetadataError>) {
        let MetadataState::Scratch(scratch) = self.state else {
            unreachable!("metadata")
        };
        let result = match native {
            Ok(()) => {
                // SAFETY: the matching original CQ ended native writes and filled output.
                unsafe { scratch.output.try_result() }
                    .map(|(kind, length, identity)| (kind, fs::Len::from(length), identity))
                    .map_err(MetadataError::Value)
            }
            Err(cause) => Err(MetadataError::Operation(Error::System(cause))),
        };
        Scratch::release(free, scratch);
        (self.file, result)
    }
}

impl<'d, R: Return<Item = read::Buffer<'d>>> read::Read<R> {
    /// Selects the remaining allocation without copying its initialized prefix.
    pub fn try_append_at(
        mut self,
        offset: fs::Offset,
    ) -> Result<Self, Rejected<Self, read::SizeError>> {
        let remaining = self.output.item().capacity() - self.output.item().len();
        // SAFETY: the private owner retains try_new's admitted capacity and prefix.
        let remaining =
            transfer::Len::from(unsafe { o3::base::num::bounded::raw::u32(remaining as u32) });
        let Some(length) = remaining.nonzero() else {
            return Err(Rejected::new(self, read::SizeError::Empty));
        };
        self.length = length;
        self.offset = offset;
        Ok(self)
    }

    pub(super) fn prepare(&mut self) {
        let prefix = self.output.item().capacity() - self.length.widen().get();
        // SAFETY: new windows begin at zero or the owned initialized length;
        // completion can only extend that prefix, and rejection cannot change it.
        unsafe { self.output.item_mut().set_len(prefix) };
    }

    pub fn output<'a>(&'a self) -> bounded::Value<&'a [u8]>
    where
        'd: 'a,
    {
        // SAFETY: the private owner retains its admitted capacity, and initialized
        // length cannot exceed it. This borrow prevents resubmission or mutation.
        bounded::Value::from_raw(unsafe { bounded::raw::Input::new(self.output.item().as_ref()) })
    }
}

pub(super) struct Entry<'d, R: Return> {
    pub(super) index: indices::SlotIndex,
    pub(super) phase: cell::Cell<Option<Phase<'d>>>,
    pub(super) waiter: cell::Cell<Option<wake::Waker<'d>>>,
    pub(super) detached: cell::Cell<bool>,
    request: cell::UnsafeCell<Option<Request<R>>>,
}

impl<'d, R: Return> Entry<'d, R> {
    pub(super) fn new(index: indices::SlotIndex) -> Self {
        Self {
            index,
            phase: cell::Cell::new(None),
            waiter: cell::Cell::new(None),
            detached: cell::Cell::new(false),
            request: cell::UnsafeCell::new(None),
        }
    }

    pub(super) fn insert(&self, request: Request<R>) {
        // SAFETY: FREE admission and take leave None. The caller owns this slot,
        // with no native borrow or application call crossing publication.
        unsafe { self.request.get().write(Some(request)) };
    }

    pub(super) fn restore(&self, phase: Option<Phase<'d>>) {
        // The prior phase was taken, or this slot was removed from FREE.
        mem::forget(self.phase.replace(phase));
    }

    pub(super) fn take(&self) -> Request<R> {
        // SAFETY: callers own a terminal/unsubmitted phase or Finish's native
        // quiescence proof. Native users and request borrows have ended.
        let Some(request) = (unsafe { &mut *self.request.get() }).take() else {
            unreachable!("request")
        };
        request
    }

    pub(super) fn with_request<T>(&self, run: impl FnOnce(&mut Request<R>) -> T) -> T {
        // SAFETY: private callers first take this slot's phase. They invoke no
        // application code and finish the loan before restoring or freeing it.
        // Submitted owners are borrowed only after their matching terminal CQ.
        let Some(request) = (unsafe { &mut *self.request.get() }).as_mut() else {
            unreachable!("request")
        };
        run(request)
    }
}

pub(super) fn submit<'d, ID: Identity, R: Return<Item = read::Buffer<'d>>>(
    entry: &Entry<'d, R>,
    slots: &flight::Slots<'d, ID>,
    free: &mut Option<Box<Scratch>>,
    driver: &mut dispatch::raw::Context<'_, 'd, dispatch::raw::Retained>,
) -> Result<Active<'d>, Error> {
    let target = driver
        .driver_ref()
        .targets::<ID>()
        .bind(entry.index, route::Epoch::INITIAL);
    entry.with_request(|request| {
        let result = match request {
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
            Request::Rename {
                directory,
                old,
                new,
            } => {
                let operation = fs::Submission::rename(directory.rename(old, new), target);
                // SAFETY: the fixed request owns the directory and both names
                // through the original completion or native quiescence.
                unsafe { driver.reborrow().submit_file(slots, operation) }.map(Active::Sync)
            }
            Request::Read(request) => {
                let output = request.output.item_mut();
                let prefix = output.len();
                // SAFETY: admission retains an initialized prefix followed by this
                // positive window. The private owner cannot move or grow its heap.
                let output = bounded::Value::from_raw(unsafe {
                    bounded::raw::Input::new(std::slice::from_raw_parts_mut(
                        output.as_mut_ptr().add(prefix).cast(),
                        request.length.widen().get(),
                    ))
                });
                let operation =
                    fs::Submission::read_at(request.file.as_fd(), output, request.offset, target);
                // SAFETY: this installed owner retains the exact FD and exclusive
                // stable heap through the original completion or native quiescence.
                unsafe { driver.reborrow().submit_file(slots, operation) }.map(Active::Read)
            }
            Request::Metadata(request) => {
                request.state = MetadataState::Scratch(Scratch::acquire(free)?);
                let MetadataState::Scratch(scratch) = &mut request.state else {
                    unreachable!("metadata")
                };
                let operation =
                    fs::Submission::metadata(request.file.as_fd(), &mut scratch.output, target);
                // SAFETY: the Box is installed in the fixed request before this
                // borrow and stays untouched through original completion or Finish.
                let result = unsafe { driver.reborrow().submit_file(slots, operation) };
                if result.is_err() {
                    request.release(free);
                }
                result.map(Active::Sync)
            }
        };
        result.map_err(Error::Submission)
    })
}

// SAFETY: installation retains the port's fixed request slots and the flight
// group; neither cancellation nor shutdown releases submitted resources.
unsafe impl<'d, ID: Identity, R: Return<Item = read::Buffer<'d>>> dispatch::raw::Manifold<'d>
    for Reader<'_, 'd, ID, R>
{
    type Route = ID;
    type Dispatch = event::Driven<dispatch::raw::Retained>;
    type Install = dispatch::raw::Plain;
    type Finish = Self;
    type Activate = dispatch::raw::Omit;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Plain;
    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self> {
        self.as_mut().get_mut().advance(turn, driver);
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self> {
        self.as_mut().get_mut().close();
        self
    }
}

// SAFETY: unit selection observes this exact installed Reader.
unsafe impl<'d, ID: Identity, R: Return<Item = read::Buffer<'d>>> dispatch::raw::Probe<'d, Self>
    for Reader<'_, 'd, ID, R>
{
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
unsafe impl<'p, 'd, ID: Identity, R: Return<Item = read::Buffer<'d>>> dispatch::raw::Control<'d>
    for Reader<'p, 'd, ID, R>
{
    type View<'step>
        = &'p read::Port<'d, R>
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

impl<'d, ID: Identity, R: Return<Item = read::Buffer<'d>>> event::Drive<'d>
    for Reader<'_, 'd, ID, R>
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>> {
        let (mut app, (), (event, _)) = call.into_parts();
        let this = app.as_mut().get_mut();
        let port = this.port;
        match event.into_kind() {
            Kind::Read(done) => {
                let Some(slot) = port.slots.get(done.token().slot().index().into_usize()) else {
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
                                Ok(amount) => {
                                    let output = request.output.item_mut();
                                    // SAFETY: the matching terminal CQ ended native access
                                    // and initialized this suffix within the admitted window.
                                    unsafe { output.set_len(output.len() + amount.into_raw()) };
                                    Ok(amount)
                                }
                                Err(cause) => Err(Error::System(cause)),
                            }
                        });
                        port.settle(&slot, Outcome::Read(result));
                    }
                    phase => {
                        entry.restore(phase);
                        return ops::ControlFlow::Break(Kind::Read(done).into());
                    }
                }
            }
            Kind::Open(done) => {
                let Some(slot) = port.slots.get(done.token().slot().index().into_usize()) else {
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
                            io::event::file::open::Outcome::Failed(cause) => Err(cause),
                        };
                        port.settle(&slot, Outcome::Open(result.map_err(Error::System)));
                    }
                    phase => {
                        entry.restore(phase);
                        return ops::ControlFlow::Break(Kind::Open(done).into());
                    }
                }
            }
            Kind::Sync(done) => {
                let Some(slot) = port.slots.get(done.token().slot().index().into_usize()) else {
                    return ops::ControlFlow::Break(Kind::Sync(done).into());
                };
                let entry = slot.state();
                match entry.phase.take() {
                    Some(Phase::Active {
                        flight: Active::Sync(flight),
                        ..
                    }) if flight.matches(done.token()) => {
                        drop(flight);
                        match entry.take() {
                            request @ Request::Rename { .. } => {
                                entry.insert(request);
                                port.settle(
                                    &slot,
                                    Outcome::Rename(done.outcome().map_err(Error::System)),
                                );
                            }
                            Request::Metadata(request) => {
                                let (file, result) =
                                    request.complete(done.outcome(), &mut this.free);
                                entry.insert(Request::Metadata(Metadata {
                                    file,
                                    state: MetadataState::Done(result),
                                }));
                                port.settle(&slot, Outcome::Metadata);
                            }
                            _ => unreachable!("operation"),
                        }
                    }
                    phase => {
                        entry.restore(phase);
                        return ops::ControlFlow::Break(Kind::Sync(done).into());
                    }
                }
            }
            other => return ops::ControlFlow::Break(other.into()),
        }
        ops::ControlFlow::Continue(())
    }
}
