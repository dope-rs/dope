use std::{marker::PhantomData, ops::ControlFlow};

use dope_sys::{
    Errno,
    engine::admit::Submitted,
    io::reactor::{
        complete::{self, Vacancy},
        ready::{self, Failure as _},
        retry::{self, Occupied as _, State as _, Vacancy as _},
    },
    table::{
        Error, Files as _,
        change::{self, Interest as _, Write as _},
    },
};

use crate::{
    output::{File, Kind, Pending, Removal, State},
    store::{
        change::Interest,
        table::{Files, Output},
    },
};

pub struct Retry<'a, C: retry::Context, E, P> {
    pub(in crate::output) files: &'a mut Files<C::Slots>,
    pub(in crate::output) state: &'a mut State<Pending<C>>,
    pub(in crate::output) poll: &'a mut P,
    pub(in crate::output) event: PhantomData<fn() -> E>,
}

pub struct Prepared<'a, C: retry::Context + 'a, E>
where
    C::Key: 'a,
{
    pub(in crate::output) owner: Removal<'a, Pending<C>>,
    pub(in crate::output) interest: Option<Interest<'a, C::Slots, { libc::EVFILT_WRITE }>>,
    pub(in crate::output) event: PhantomData<fn() -> E>,
}

impl<'a, C, E> complete::Cancellation for Prepared<'a, C, E>
where
    C: retry::Context + 'a,
    C::Key: 'a,
    E: retry::Complete<C>,
{
    type Event = E;

    fn commit(self, vacant: impl Vacancy<Event = E>) -> Submitted {
        let Self {
            owner,
            interest,
            event: _,
        } = self;
        if let Some(interest) = interest {
            interest.cancel();
        }
        vacant.push(owner.remove().rejected(Errno::CANCELED));
        Submitted
    }
}

impl<C, E, P> Retry<'_, C, E, P>
where
    C: retry::Context,
    E: retry::Complete<C>,
    P: ready::Poll<
            C::Slots,
            Files = Files<C::Slots>,
            Failure: ready::Failure<Operation = C::Operation>,
        >,
{
    pub(in crate::output) fn file<'a>(
        files: &'a mut Files<C::Slots>,
        poll: &mut P,
        slot: <C::Slots as change::raw::Slots>::Physical,
        op: C::Operation,
    ) -> ControlFlow<(), Output<'a, C::Slots>> {
        poll.execute(
            || files.output(slot),
            |error| match error {
                Error::Missing(cause) => P::Failure::missing(op, cause),
                Error::Index(error) => P::Failure::backing(error),
            },
        )
    }

    pub(in crate::output) fn arm(
        &mut self,
        file: File<C>,
        key: C::Key,
        kind: Kind<C>,
        errno: Errno,
        vacant: impl Vacancy<Event = E>,
    ) {
        let pending = Pending {
            target: key,
            file,
            kind,
        };
        let file = match self
            .poll
            .execute(|| self.files.write(file), P::Failure::backing)
        {
            ControlFlow::Continue(Some(file)) => file,
            ControlFlow::Continue(None) => {
                vacant.push(pending.rejected(Errno::BAD_DESCRIPTOR));
                return;
            }
            ControlFlow::Break(()) => return,
        };
        Self::insert(self.state, file, pending, errno, vacant);
    }

    pub(in crate::output) fn insert(
        state: &mut State<Pending<C>>,
        file: Interest<'_, C::Slots, { libc::EVFILT_WRITE }>,
        pending: Pending<C>,
        errno: Errno,
        vacant: impl Vacancy<Event = E>,
    ) {
        let key = pending.target;
        let vacancy = match state.vacancy(pending) {
            Ok(vacancy) => vacancy,
            Err(pending) => {
                vacant.push(pending.rejected(errno));
                return;
            }
        };
        file.arm(libc::EV_ADD | libc::EV_CLEAR | libc::EV_ONESHOT, key.into());
        vacancy.commit();
    }
}
