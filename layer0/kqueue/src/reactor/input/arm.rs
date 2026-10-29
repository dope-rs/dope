use std::{marker::PhantomData, ops::ControlFlow, ptr};

use dope_sys::{
    Errno,
    engine::admit::Submitted,
    io::{
        reactor::{
            complete::{self, Vacancy},
            read::{self, Call as _, Enabled, Family},
            ready::{self, Failure as _},
        },
        request::Datagram,
        socket::accept,
    },
    table::{
        Error, Files as _,
        change::{self, Interest as _},
        registry::{self, Entry as _, Occupied as _, State as _, Vacancy as _},
    },
};
use libc::socklen_t;

use crate::{
    reactor::input::{
        File, Slot,
        entry::{Id, Kind},
    },
    store::{
        change::Interest,
        registry::{Occupied, Registry},
        table::Files,
    },
};

pub struct Arm<'a, C: read::Context, E, P> {
    pub(super) files: &'a mut Files<C::Slots>,
    pub(super) reads: &'a mut Registry<C>,
    pub(super) poll: &'a mut P,
    event: PhantomData<fn() -> E>,
}

pub struct Prepared<'a, C: read::Context, E> {
    owner: Occupied<'a, C>,
    interest: Option<Interest<'a, C::Slots, { libc::EVFILT_READ }>>,
    event: PhantomData<fn() -> E>,
}

impl<C, E> complete::Cancellation for Prepared<'_, C, E>
where
    C: read::Context,
    E: read::Complete<Key = C::Key, Flow = C::Flow>,
{
    type Event = E;

    fn commit(self, vacant: impl Vacancy<Event = E>) -> Submitted {
        let Self {
            owner,
            interest,
            event: _,
        } = self;
        if let Some(interest) = interest {
            interest.delete();
        }
        let call = owner.remove().into_call();
        fail(call.key(), call.family(), Errno::CANCELED, vacant);
        Submitted
    }
}

impl<'a, C, E, P> read::Arm<'a, C, E, P> for Arm<'a, C, E, P>
where
    C: read::Context,
    E: read::Complete<Key = C::Key, Flow = C::Flow>,
    P: ready::Poll<
            C::Slots,
            Files = Files<C::Slots>,
            Failure: ready::Failure<Operation = C::Operation>,
        >,
{
    type Files = Files<C::Slots>;
    type Entry = Slot<C>;
    type Registry = Registry<C>;
    type Failure = P::Failure;
    type Prepared = Prepared<'a, C, E>;

    fn new(files: &'a mut Self::Files, reads: &'a mut Self::Registry, poll: &'a mut P) -> Self {
        Self {
            files,
            reads,
            poll,
            event: PhantomData,
        }
    }

    fn accept_one(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        addr: &mut libc::sockaddr_storage,
        len: &mut socklen_t,
        vacant: impl Vacancy<Event = E>,
    ) {
        self.start(
            slot,
            Kind::Accept,
            |file| {
                <Slot<C> as read::raw::Entry<C>>::accept(
                    file,
                    key,
                    ptr::from_mut(addr),
                    len,
                    E::LAST,
                )
            },
            vacant,
        );
    }

    fn accept_many(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        vacant: impl Vacancy<Event = E>,
    ) {
        self.start(
            slot,
            Kind::Accept,
            |file| {
                <Slot<C> as read::raw::Entry<C>>::accept(
                    file,
                    key,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    E::MORE,
                )
            },
            vacant,
        );
    }

    fn recv(
        &mut self,
        key: C::Key,
        slot: <C::Slots as change::raw::Slots>::Physical,
        vacant: impl Vacancy<Event = E>,
    ) {
        self.start(
            slot,
            Kind::Recv,
            |file| <Slot<C> as read::raw::Entry<C>>::recv(file, key),
            vacant,
        );
    }

    fn message(
        &mut self,
        key: C::Key,
        message: impl Datagram<Target = <C::Slots as change::raw::Slots>::Physical>,
        vacant: impl Vacancy<Event = E>,
    ) {
        self.start(
            message.into_target(),
            Kind::Message,
            |file| <Slot<C> as read::raw::Entry<C>>::message(file, key),
            vacant,
        );
    }

    fn prepare(self, key: C::Key, family: Family) -> Result<Option<Self::Prepared>, Self::Failure> {
        self.poll.check()?;
        let Some(owner) = self.reads.occupied(key, family) else {
            return Ok(None);
        };
        let interest = self
            .files
            .read(owner.get().file())
            .map_err(P::Failure::backing)?;
        Ok(Some(Prepared {
            owner,
            interest,
            event: PhantomData,
        }))
    }

    fn enable(&mut self, file: File<C>, key: C::Key) -> Enabled {
        match self
            .poll
            .execute(|| self.files.read(file), P::Failure::backing)
        {
            ControlFlow::Continue(Some(file)) => {
                file.arm(libc::EV_ENABLE | libc::EV_DISPATCH, key.into());
                Enabled::Ready
            }
            ControlFlow::Continue(None) => Enabled::Stale,
            ControlFlow::Break(()) => Enabled::Failed,
        }
    }

    fn finish(&mut self, id: Id) {
        if let Some(call) = self.reads.take(id) {
            self.delete(call.file());
        }
    }
}

impl<C, E, P> Arm<'_, C, E, P>
where
    C: read::Context,
    E: read::Complete<Key = C::Key, Flow = C::Flow>,
    P: ready::Poll<
            C::Slots,
            Files = Files<C::Slots>,
            Failure: ready::Failure<Operation = C::Operation>,
        >,
{
    fn start(
        &mut self,
        fixed: <C::Slots as change::raw::Slots>::Physical,
        kind: Kind,
        make: impl FnOnce(File<C>) -> Slot<C>,
        vacant: impl Vacancy<Event = E>,
    ) {
        let op = kind.operation::<C>();
        let file = match self.poll.execute(
            || self.files.try_read(fixed),
            |error| match error {
                Error::Missing(cause) => P::Failure::missing(op, cause),
                Error::Index(error) => P::Failure::backing(error),
            },
        ) {
            ControlFlow::Continue(file) => file,
            ControlFlow::Break(()) => return,
        };
        let slot = make(file.id());
        let vacancy = match self.reads.try_vacant(slot) {
            Ok(vacancy) => vacancy,
            Err((slot, cause)) => {
                let call = slot.operation();
                match call.family() {
                    Family::Accept => vacant.push(E::rejected(
                        call.key(),
                        accept::Error::Registration(cause),
                        E::LAST,
                    )),
                    Family::Recv => {
                        let errno = match cause {
                            registry::Failure::Slot => Errno::BAD_DESCRIPTOR,
                            registry::Failure::TargetOccupied | registry::Failure::FileOccupied => {
                                Errno::ALREADY_EXISTS
                            }
                            registry::Failure::TargetFull => Errno::TOO_MANY_FILES,
                        };
                        vacant.push(E::control(call.key(), Err(errno), E::LAST));
                    }
                }
                return;
            }
        };
        let echo = vacancy.insert();
        file.arm(
            libc::EV_ADD | libc::EV_ENABLE | libc::EV_DISPATCH,
            echo.into(),
        );
    }

    fn delete(&mut self, file: File<C>) {
        if let ControlFlow::Continue(Some(file)) = self
            .poll
            .execute(|| self.files.read(file), P::Failure::backing)
        {
            file.delete();
        }
    }
}

pub(super) fn fail<E: read::Complete>(
    key: E::Key,
    family: Family,
    errno: Errno,
    vacant: impl Vacancy<Event = E>,
) {
    match family {
        Family::Accept => vacant.push(E::rejected(key, accept::Error::Syscall(errno), E::LAST)),
        Family::Recv => vacant.push(E::control(key, Err(errno), E::LAST)),
    }
}
