use std::convert;

use dope_core::driver::{self, schedule};
use dope_net::link::egress;

use crate::connector::{self, app, connection};

mod policy;

pub(crate) use policy::Policy;

pub trait Mode<'d, const ID: u8, A>:
    Policy<'d, ID, A, Outcome = <Self as Mode<'d, ID, A>>::Outcome>
where
    A: app::Receive<'d, ID>,
{
    type Outcome;
}

pub struct Complete;
pub struct Resumable;

impl<'d, const ID: u8, A> Policy<'d, ID, A> for Complete
where
    A: app::Receive<'d, ID, Continuation = Self>,
{
    type Outcome = app::ChunkOutcome;
    type Permit<'turn> = convert::Infallible;

    fn dispatch<R, O, C, Y>(outcome: Self::Outcome, receiver: R, complete: C, _: Y) -> O
    where
        C: FnOnce(R, app::ChunkOutcome) -> O,
        Y: FnOnce(R) -> O,
    {
        complete(receiver, outcome)
    }

    fn admit<'turn, R, O, C, D, N>(
        _: bool,
        _: schedule::Application<'turn, 'd>,
        receiver: R,
        _: C,
        _: D,
        absent: N,
    ) -> O
    where
        C: FnOnce(R, Self::Permit<'turn>) -> O,
        D: FnOnce(R) -> O,
        N: FnOnce(R) -> O,
    {
        absent(receiver)
    }

    fn resume<'turn, O>(
        permit: Self::Permit<'turn>,
        _: &mut A,
        _: connection::Ctx<'_, 'd, ID, A::Wire, A::Conn, O>,
        _: egress::Queue<'_, 'd, { connector::IOV_CAP }, A::Send>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::ChunkOutcome {
        match permit {}
    }
}

impl<'d, const ID: u8, A> Mode<'d, ID, A> for Complete
where
    A: app::Receive<'d, ID, Continuation = Self>,
{
    type Outcome = app::ChunkOutcome;
}

impl<'d, const ID: u8, A> Policy<'d, ID, A> for Resumable
where
    A: app::ResumableReceive<'d, ID>,
{
    type Outcome = app::ResumeOutcome;
    type Permit<'turn> = schedule::ApplicationPermit<'turn, 'd>;

    fn dispatch<R, O, C, Y>(outcome: Self::Outcome, receiver: R, complete: C, yielded: Y) -> O
    where
        C: FnOnce(R, app::ChunkOutcome) -> O,
        Y: FnOnce(R) -> O,
    {
        use app::ResumeOutcome;

        match outcome {
            ResumeOutcome::Complete(outcome) => complete(receiver, outcome),
            ResumeOutcome::Yield => yielded(receiver),
        }
    }

    fn admit<'turn, R, O, C, D, N>(
        pending: bool,
        work: schedule::Application<'turn, 'd>,
        receiver: R,
        ready: C,
        deferred: D,
        absent: N,
    ) -> O
    where
        C: FnOnce(R, Self::Permit<'turn>) -> O,
        D: FnOnce(R) -> O,
        N: FnOnce(R) -> O,
    {
        if !pending {
            return absent(receiver);
        }
        match work.permit() {
            Some(permit) => ready(receiver, permit),
            None => deferred(receiver),
        }
    }

    fn resume<'turn, O>(
        permit: Self::Permit<'turn>,
        app: &mut A,
        connection: connection::Ctx<'_, 'd, ID, A::Wire, A::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, A::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::ResumeOutcome {
        app.resume(permit, connection, egress, driver)
    }
}

impl<'d, const ID: u8, A> Mode<'d, ID, A> for Resumable
where
    A: app::ResumableReceive<'d, ID>,
{
    type Outcome = app::ResumeOutcome;
}
