use std::ops::ControlFlow;

use dope_sys::{
    Errno,
    io::reactor::{
        complete::Vacancy,
        read,
        read::{Arm as _, Call as _, Enabled, Outcome},
        ready,
        ready::Failure as _,
    },
    memory, table,
    table::registry::{Query, State as _},
};

use crate::{
    reactor::input::{self, Arm, Poll, arm::fail, entry::Slot},
    store::{
        buffer::{Buffer, Pool},
        table::Files,
    },
};

impl<'a, C, L, E, P> read::raw::Poll<'a, C, L, E, P> for Poll<'a, C, L, E, P>
where
    C: read::Context,
    L: memory::raw::Layout,
    E: read::Complete<
            Key = C::Key,
            Flow = C::Flow,
            Accepted = <C::Slots as table::raw::Slots>::Accepted,
            Buffer = Buffer,
        >,
    P: ready::Poll<
            C::Slots,
            Files = Files<C::Slots>,
            Failure: ready::Failure<Operation = C::Operation>,
        >,
{
    type Arm = Arm<'a, C, E, P>;
    type Pool = Pool<L>;

    fn new(arm: Self::Arm, pool: &'a Self::Pool, query: Query<Slot<C>>) -> Option<Self> {
        let (id, call) = match query {
            Query::Exact(id) => (id, arm.reads.operation(id)?),
            Query::Echo(key) => arm.reads.resolve(key)?,
        };
        Some(Self {
            arm,
            pool,
            id,
            call,
        })
    }

    unsafe fn dispatch(
        mut self,
        filter: i16,
        error: Option<Errno>,
        vacant: impl Vacancy<Event = E>,
    ) {
        let fault = if filter != libc::EVFILT_READ {
            Some(Errno::PROTOCOL)
        } else {
            error
        };
        if let Some(errno) = fault {
            self.arm.finish(self.id);
            fail(self.call.key(), self.call.family(), errno, vacant);
            return;
        }
        unsafe { self.resume(vacant) };
    }

    unsafe fn resume(self, vacant: impl Vacancy<Event = E>) {
        let Self {
            mut arm,
            pool,
            id,
            call,
        } = self;
        let op = call.kind().operation::<C>();
        let outcome = match arm.poll.execute(
            || unsafe { input::raw::Drain::<(C, L, E)>::resume(arm.files, pool, call, vacant) },
            |cause| P::Failure::missing(op, cause),
        ) {
            ControlFlow::Continue(outcome) => outcome,
            ControlFlow::Break(()) => return,
        };
        match outcome {
            Outcome::Rearm => match arm.enable(call.file(), call.key()) {
                Enabled::Ready => {}
                Enabled::Stale => arm.finish(id),
                Enabled::Failed => {}
            },
            Outcome::Yield => arm.reads.queue_resume(id),
            Outcome::Terminal => arm.finish(id),
        }
    }
}
