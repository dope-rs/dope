use std::{marker, mem};

use dope_sys::{
    engine,
    io::socket::option::tuning::{Error, device},
};
use o3::depot::arena::slab::Vacancy;

use crate::{
    Engine, Errno, driver,
    driver::{
        result,
        retained::{flight, flight::cancel},
        route::{indices, kind},
    },
    io::{
        event::net::{connect, tuning},
        fd::handles,
        socket::option,
    },
};

#[doc(hidden)]
pub struct Live;

#[doc(hidden)]
pub struct Cancelled;

/// Exact cause of a socket submission rejection.
#[derive(Debug)]
pub enum Cause {
    Submit(result::SubmitError),
    Option(Errno),
}

#[must_use = "a rejected establishment retains its exact descriptor and cause"]
pub struct Rejection<'d> {
    fd: handles::Descriptor<'d>,
    cause: Cause,
}

#[must_use = "cancellation either transfers the item to the kernel or returns it for retry"]
pub enum Cancellation<'d> {
    Submitted(Tuning<'d, Cancelled>),
    Retry {
        item: Tuning<'d>,
        error: result::SubmitError,
    },
}

#[doc(hidden)]
#[must_use = "a pending establishment owns its descriptor and completion authority"]
#[repr(C)]
pub struct Tuning<'d, State = Live> {
    fd: handles::Descriptor<'d>,
    flight: flight::Flight<'d, kind::Tuning>,
    permit: <Engine as engine::Cancel<indices::SlotIndex>>::Permit,
    state: marker::PhantomData<State>,
}

#[doc(hidden)]
#[must_use = "a pending establishment owns its descriptor and completion authority"]
#[repr(C)]
pub struct Connection<'d, State = Live> {
    fd: handles::Descriptor<'d>,
    flight: flight::Flight<'d, kind::Connect>,
    state: marker::PhantomData<State>,
}

#[must_use = "a completion mismatch retains both pending and completion authority"]
pub struct Mismatch<T, C> {
    item: T,
    done: C,
}

impl<T, C> Mismatch<T, C> {
    pub fn into_parts(self) -> (T, C) {
        (self.item, self.done)
    }
}

impl<'d, State> Tuning<'d, State> {
    #[doc(hidden)]
    pub const fn fd(&self) -> &handles::Descriptor<'d> {
        &self.fd
    }
}

impl<'d> Tuning<'d> {
    pub(crate) fn from_native<P>(
        finish: device::Finish<handles::Descriptor<'d>, flight::Reserved<'d, kind::Tuning>, P>,
    ) -> option::Tuning<'d>
    where
        Engine: engine::Cancel<indices::SlotIndex, Permit = P>,
    {
        match finish {
            device::Finish::Applied(fd) => option::Tuning::Applied(fd),
            device::Finish::Pending(fd, target, permit) => option::Tuning::Pending(Self {
                fd,
                flight: target.commit(),
                permit,
                state: marker::PhantomData,
            }),
        }
    }

    fn commit<P>(self) -> Tuning<'d, Cancelled>
    where
        Engine: engine::Cancel<indices::SlotIndex, Permit = P>,
    {
        Tuning {
            fd: self.fd,
            flight: self.flight,
            permit: self.permit,
            state: marker::PhantomData,
        }
    }

    pub fn cancel(self, driver: &mut driver::Context<'_, 'd>) -> Cancellation<'d> {
        match engine::Cancel::cancel(driver.native().as_mut(), &self.permit, self.fd.index()) {
            Ok(()) => Cancellation::Submitted(self.commit()),
            Err(error) => Cancellation::Retry {
                item: self,
                error: result::SubmitError::queue(error),
            },
        }
    }
}

impl<'d, State> Connection<'d, State> {
    #[doc(hidden)]
    pub const fn fd(&self) -> &handles::Descriptor<'d> {
        &self.fd
    }
}

impl<'d> Connection<'d> {
    pub(crate) fn connect(
        fd: handles::Descriptor<'d>,
        flight: flight::Flight<'d, kind::Connect>,
    ) -> Self {
        Self {
            fd,
            flight,
            state: marker::PhantomData,
        }
    }

    pub fn try_cancel(
        mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Connection<'d, Cancelled>, (Self, cancel::Error)> {
        match driver.native().try_stop_at(&mut self.flight, &self.fd) {
            Ok(_) => Ok(self.commit()),
            Err(error) => Err((self, error)),
        }
    }

    pub(crate) fn commit(self) -> Connection<'d, Cancelled> {
        Connection {
            fd: self.fd,
            flight: self.flight,
            state: marker::PhantomData,
        }
    }
}

impl<'d> Rejection<'d> {
    pub(crate) const fn new(fd: handles::Descriptor<'d>, cause: Cause) -> Self {
        Self { fd, cause }
    }

    pub fn into_parts(self) -> (handles::Descriptor<'d>, Cause) {
        (self.fd, self.cause)
    }
}

impl<'d> From<Error<handles::Descriptor<'d>, Vacancy>> for Rejection<'d> {
    fn from(error: Error<handles::Descriptor<'d>, Vacancy>) -> Self {
        let (fd, cause) = match error {
            Error::Full(fd, error) => (fd, Cause::Submit(result::SubmitError::queue(error))),
            Error::Reserve(fd, error) => (fd, Cause::Submit(result::SubmitError::from(error))),
        };
        Self::new(fd, cause)
    }
}

impl<'d, State> Tuning<'d, State> {
    #[doc(hidden)]
    pub fn complete(
        self,
        done: tuning::Completion<'d>,
    ) -> Result<(handles::Descriptor<'d>, Result<(), Errno>), Mismatch<Self, tuning::Completion<'d>>>
    {
        let outcome = match done.resolve(self.flight.target()) {
            Ok(outcome) => outcome,
            Err(done) => return Err(Mismatch { item: self, done }),
        };
        drop(self.flight);
        Ok((self.fd, outcome))
    }
}

impl<'d, State> Connection<'d, State> {
    #[doc(hidden)]
    pub fn complete(
        self,
        done: connect::Completion<'d>,
    ) -> Result<(handles::Descriptor<'d>, Result<(), Errno>), Mismatch<Self, connect::Completion<'d>>>
    {
        let outcome = match done.resolve(self.flight.target()) {
            Ok(outcome) => outcome,
            Err(done) => return Err(Mismatch { item: self, done }),
        };
        drop(self.flight);
        Ok((self.fd, outcome))
    }
}

const _: () = {
    assert!(mem::size_of::<Live>() == 0);
    assert!(mem::size_of::<Cancelled>() == 0);
    assert!(
        mem::size_of::<Tuning<'_>>()
            == mem::size_of::<(handles::Descriptor<'_>, flight::Flight<'_>)>()
    );
    assert!(
        mem::align_of::<Tuning<'_>>()
            == mem::align_of::<(handles::Descriptor<'_>, flight::Flight<'_>)>()
    );
    assert!(mem::offset_of!(Tuning<'_>, fd) == 0);
    assert!(
        mem::size_of::<Connection<'_>>()
            == mem::size_of::<(handles::Descriptor<'_>, flight::Flight<'_, kind::Connect>)>()
    );
    assert!(mem::offset_of!(Connection<'_>, fd) == 0);
    assert!(mem::size_of::<Rejection<'_>>() == mem::size_of::<(handles::Descriptor<'_>, Cause)>());
    assert!(
        mem::align_of::<Rejection<'_>>() == mem::align_of::<(handles::Descriptor<'_>, Cause)>()
    );
    assert!(mem::size_of::<Tuning<'_, Cancelled>>() == mem::size_of::<Tuning<'_>>());
    assert!(mem::size_of::<Connection<'_, Cancelled>>() == mem::size_of::<Connection<'_>>());
};

impl<'d> From<device::Failure<handles::Descriptor<'d>, Vacancy>> for Rejection<'d> {
    fn from(error: device::Failure<handles::Descriptor<'d>, Vacancy>) -> Self {
        match error {
            device::Failure::Option(fd, error) => Self::new(fd, Cause::Option(error)),
            device::Failure::Reserve(error) => error.into(),
        }
    }
}
