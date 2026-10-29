use dope_sys::{
    io::{
        file,
        reactor::{
            complete::flags::{Flow, Meta as _},
            submit::Control as _,
        },
        request::Lower as _,
    },
    kernel, memory, probe,
    probe::{
        Context as _, Outcome as _, Stopped, accept,
        accept::{Accept as _, Range as _},
        cleanup::Cleanup as _,
    },
    table::{token, token::host},
};

use crate::{
    Observed, Opened, Ring, Uring, error,
    reactor::{
        op::sockets,
        register,
        ring::{Accepted, Outcome, Range, Recover, Waiting},
        sq,
    },
    sockets::probe::Loopback,
};

type Identity<T> = <<T as token::Codec>::Context as host::Context>::Probe;
type Cause<T> = probe::Cause<Identity<T>, Observed<T>, kernel::Errno<error::Enter>>;
type Failure<T, L, P> = Result<P, Recover<T, L, P>>;

pub(crate) struct Listener<T: token::Codec> {
    pub(in crate::reactor::ring) ring: Ring<T>,
    pub(in crate::reactor::ring) link: Loopback,
    pub(in crate::reactor::ring) range: Range,
    pub(in crate::reactor::ring) file: register::raw::Occupied,
}

impl<T: token::Codec> Listener<T>
where
    T::Context: probe::Context,
{
    pub(in crate::reactor::ring) fn direct<L, P>(mut self) -> Result<Self, Failure<T, L, P>>
    where
        L: memory::raw::Canary,
        P: Copy
            + From<Cause<T>>
            + From<probe::Syscall<Identity<T>, dope_sys::Errno>>
            + From<accept::Check<Identity<T>>>,
    {
        const {
            assert!(
                size_of::<Self>()
                    == size_of::<(Ring<T>, Loopback, Range, register::raw::Occupied)>()
            );
            assert!(
                align_of::<Self>()
                    == align_of::<(Ring<T>, Loopback, Range, register::raw::Occupied)>()
            );
        }
        match self.try_direct::<P>() {
            Ok(()) => Ok(self),
            Err(cause) => Err(self.fail(cause)),
        }
    }

    fn try_direct<P>(&mut self) -> Result<(), P>
    where
        P: Copy
            + From<Cause<T>>
            + From<probe::Syscall<Identity<T>, dope_sys::Errno>>
            + From<accept::Check<Identity<T>>>,
    {
        let probe = T::Context::ACCEPT;
        let client = self
            .link
            .connect()
            .map_err(|error| probe::Syscall { probe, error })?;
        let entry = sockets::accept(self.range.fixed()).user_data(T::probe(probe));
        let done = self.submit(&entry, probe)?;
        <Opened as file::raw::Completion>::close(client)
            .map_err(|error| probe::Syscall { probe, error })?;
        let result = done.success().map_err(Cause::<T>::Check)?;
        let slot = self
            .range
            .accepted(result)
            .map_err(|cause| accept::Check::Outside { probe, cause })?;
        self.close(slot, T::Context::ACCEPT_CLOSE).map_err(P::from)
    }

    pub(in crate::reactor::ring) fn multishot<L, P>(self) -> Result<Self, Failure<T, L, P>>
    where
        L: memory::raw::Canary,
        P: Copy
            + From<Cause<T>>
            + From<probe::Syscall<Identity<T>, dope_sys::Errno>>
            + From<accept::Check<Identity<T>>>,
    {
        let probe = T::Context::MULTISHOT;
        let client = match self.link.connect() {
            Ok(client) => client,
            Err(error) => return Err(self.fail(probe::Syscall { probe, error }.into())),
        };
        let entry = sq::Entry::accept_multi(self.range.fixed()).user_data(T::probe(probe));
        let Self {
            ring,
            link,
            range,
            file,
        } = self;
        let (active, done) = match Accepted::new(ring, probe).submit(entry) {
            Ok(done) => done,
            Err(stopped) => return Err(Recover::accept(stopped.state, stopped.cause.into(), file)),
        };
        let closed = <Opened as file::raw::Completion>::close(client)
            .map_err(|error| probe::Syscall { probe, error });
        let active = match Stopped::attach(active, closed) {
            Ok((active, ())) => active,
            Err(stopped) => return Err(Recover::accept(stopped.state, stopped.cause.into(), file)),
        };
        let flow = done.meta().flow();
        let result = match done.success() {
            Ok(result) => result,
            Err(error) => {
                return Err(Recover::accept(
                    active,
                    Cause::<T>::Check(error).into(),
                    file,
                ));
            }
        };
        if flow == Flow::Last {
            return Err(Recover::accept(
                active,
                accept::Check::Early(probe).into(),
                file,
            ));
        }
        let slot = match range.accepted(result) {
            Ok(slot) => slot,
            Err(cause) => {
                return Err(Recover::accept(
                    active,
                    accept::Check::Outside { probe, cause }.into(),
                    file,
                ));
            }
        };
        let ring = match active.cancel() {
            Ok(ring) => ring,
            Err(stopped) => return Err(Recover::accept(stopped.state, stopped.cause.into(), file)),
        };
        let mut state = Self {
            ring,
            link,
            range,
            file,
        };
        match state.close(slot, T::Context::MULTI_CLOSE) {
            Ok(()) => Ok(state),
            Err(cause) => Err(state.fail(cause.into())),
        }
    }

    fn submit(
        &mut self,
        entry: &sq::Owned<T>,
        probe: Identity<T>,
    ) -> Result<Outcome<T, Identity<T>>, Cause<T>> {
        use dope_sys::probe::raw::Wait as _;
        let waiting = Waiting::submit(self, |state| &mut state.ring, entry, probe)
            .map_err(|stopped| stopped.cause)?;
        waiting
            .finish(|state| &mut state.ring)
            .map(|(_, done)| done)
            .map_err(|stopped| stopped.cause)
    }

    fn close(&mut self, slot: sq::Fixed, probe: Identity<T>) -> Result<(), Cause<T>> {
        let entry = Uring::close(slot, T::probe(probe));
        self.submit(&entry, probe)?
            .zero()
            .map_err(Cause::<T>::Check)
    }

    fn fail<L: memory::raw::Canary, P: Copy>(self, primary: P) -> Failure<T, L, P> {
        Recover::file(self.ring, primary, self.file)
    }
}
