use dope_sys::{
    io::reactor::submit::Control as _,
    kernel,
    probe::{Cause, Outcome as _, Stopped, accept},
    ring::Owner as _,
    table::token::{self, host},
};

use crate::{
    Observed, Ring, Uring, error,
    reactor::{
        register,
        ring::{Outcome, Waiting},
        sq,
    },
};

type Identity<T> = <<T as token::Codec>::Context as host::Context>::Probe;
type Fault<T> = Cause<Identity<T>, Observed<T>, kernel::Errno<error::Enter>>;
type Checked<T> = Outcome<T, Identity<T>>;

#[must_use = "a live accept canary must reach its terminal completion"]
pub struct Accepted<T: token::Codec> {
    ring: Ring<T>,
    probe: Identity<T>,
}

impl<T: token::Codec> accept::Accept for Accepted<T> {
    type Ring = Ring<T>;
    type Owned = sq::Owned<T>;
    type Probe = Identity<T>;
    type Outcome = Checked<T>;
    type Error = kernel::Errno<error::Enter>;
    type File = register::raw::Occupied;
    type Closing = register::Closing<T>;

    fn new(ring: Ring<T>, probe: Identity<T>) -> Self {
        Self { ring, probe }
    }

    fn submit(self, entry: sq::Owned<T>) -> Result<(Self, Checked<T>), Stopped<Self, Fault<T>>> {
        use dope_sys::probe::raw::Wait as _;
        let probe = self.probe;
        Waiting::submit(self, Self::ring, &entry, probe)?.finish(Self::ring)
    }

    fn cancel(self) -> Result<Ring<T>, Stopped<Self, Fault<T>>> {
        use dope_sys::probe::raw::Wait as _;
        let cancel = Uring::cancel(T::probe(self.probe), T::cancel_probe(self.probe));
        let probe = self.probe;
        let (state, done) =
            Waiting::submit(self, Self::ring, &cancel, probe)?.finish(Self::ring)?;
        let (state, ()) = Stopped::attach(state, done.canceled().map_err(Cause::Check))?;
        Ok(state.ring)
    }

    fn try_close(self, file: Self::File) -> Result<(), register::Closing<T>> {
        self.ring.close_file(file)
    }
}

impl<T: token::Codec> Accepted<T> {
    fn ring(&mut self) -> &mut Ring<T> {
        &mut self.ring
    }
}
