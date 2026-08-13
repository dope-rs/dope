use dope_core::driver::{self, schedule};
use dope_net::link::egress;

use crate::connector::{self, app, connection};

pub trait Policy<'d, const ID: u8, A>
where
    A: app::Receive<'d, ID>,
{
    type Outcome;
    type Permit<'turn>;

    fn dispatch<R, O, C, Y>(outcome: Self::Outcome, receiver: R, complete: C, yielded: Y) -> O
    where
        C: FnOnce(R, app::ChunkOutcome) -> O,
        Y: FnOnce(R) -> O;

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
        N: FnOnce(R) -> O;

    fn resume<'turn, O>(
        permit: Self::Permit<'turn>,
        app: &mut A,
        connection: connection::Ctx<'_, 'd, ID, A::Wire, A::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, A::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Self::Outcome;
}
