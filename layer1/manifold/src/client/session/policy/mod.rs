//! Service-bound close and executor policies.

use core::convert::Infallible;

use dope_core::driver::route::Identity;

pub mod admit;
pub mod end;
pub mod output;
pub mod start;

use app::admit::Mode;
use bindings::endpoint::State;
use dope_core::{
    driver::{
        self,
        schedule::{Progress, app::Work, clock},
    },
    io::socket,
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire,
};
use o3::base::{Call, cell::region, time::Positive};

use crate::{
    client::{
        app::{self, connect, failure, request},
        attempt, close, conn,
        session::{self, app::drain, protocol, turns},
    },
    service::bindings,
};

/// Exposes the final consumers for one wire's exact failure classes.
pub trait Compatible<'w, 'd, W, ID: Identity>: session::Session<'d, ID>
where
    W: wire::Spec + 'w,
{
    type Open: start::Sink<'w, 'd, W, ID> + ?Sized;
    type Terminal: end::Sink<'w, 'd, W, ID> + ?Sized;

    fn open(&self) -> &Self::Open;

    fn terminal(&self) -> &Self::Terminal;
}

pub trait Output<'life, 'd: 'life, P: ?Sized + 'life, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX> + Sized
{
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut State<'life, 'd, P, Self, ID, MAX>,
        &'c mut session::Context<'t, 'e, 'life, 'd, P, Self, ID, MAX>,
    )>;

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut State<'life, 'd, P, Self, ID, MAX>,
        conn::Id<'d, ID>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<Infallible>::None
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a State<'life, 'd, P, Self, ID, MAX>,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        use std::mem;
        output::Cancel::<_>::Release(mem::drop::<Self::Send>)
    }
}

pub trait Requests<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX, Requests = request::Enabled>
{
    fn drain<'e>(
        owner: &State<'life, 'd, P, Self, ID, MAX>,
        reqs: &mut request::Requests<'_, '_, 'e, 'd, Self::Send, ID>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    );
}

pub trait Lifecycle<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX>
{
    type Drain;

    fn io_fail<'e>(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        error: dope_core::Errno,
        ctx: &mut session::Context<'_, 'e, 'life, 'd, P, Self, ID, MAX>,
    );

    fn quarantine(
        owner: &State<'life, 'd, P, Self, ID, MAX>,
        conn: conn::Id<'d, ID>,
        state: &mut Self::ConnState,
        reason: close::Reason,
        region: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    );

    fn clear_send<'turn>(
        _: &State<'life, 'd, P, Self, ID, MAX>,
        _: conn::Id<'d, ID>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn clear_recv<'turn>(
        _: &State<'life, 'd, P, Self, ID, MAX>,
        _: conn::Id<'d, ID>,
        _: close::Reason,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn fence(
        _: &State<'life, 'd, P, Self, ID, MAX>,
        _: conn::Id<'d, ID>,
        _: &Self::ConnState,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        app::Fence::Ready
    }

    fn policy(
        _: &State<'life, 'd, P, Self, ID, MAX>,
        _: conn::Id<'d, ID>,
        _: &Self::ConnState,
        _: close::Reason,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> close::Policy {
        close::Policy::Reconnect
    }

    fn disconnect(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        ctx: turns::Retired<'_, 'd, Self::ConnState, ID, State<'life, 'd, P, Self, ID, MAX>>,
    );
}

pub trait Quiescence<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    Lifecycle<'life, 'd, P, ID, MAX, Drain = drain::Application>
{
    fn quiesce<'a>(
        owner: &State<'life, 'd, P, Self, ID, MAX>,
        barrier: app::Barrier<'a, 'd, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> app::Drain<'a, 'd, ID>;
}

pub trait Connected<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX, Connect = connect::Full>
{
    type Activation: Mode;

    fn connect<'e>(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        key: attempt::Completion<'_, 'd, ID>,
        peer: socket::Addr,
        ctx: turns::Startup<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    );
}

pub trait Peer<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX, Connect = connect::Peer>
{
    type Addr;

    fn connect<'e>(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        peer: Self::Addr,
        ctx: &mut session::Context<'_, 'e, 'life, 'd, P, Self, ID, MAX>,
    );
}

pub trait Exhausted<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX>
{
    fn exhaust(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        key: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Failed<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX, Failure = failure::Observed>
{
    fn failed(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        key: attempt::Completion<'_, 'd, ID>,
        cause: event::ConnectFault,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Scheduling<'life, 'd: 'life, P: ?Sized, ID: Identity, const MAX: usize>:
    protocol::Spec<'life, 'd, P, ID, MAX>
{
    fn park<'turn>(
        owner: &mut State<'life, 'd, P, Self, ID, MAX>,
        work: Work<'turn, 'd>,
        region: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    );

    fn shutdown(owner: &mut State<'life, 'd, P, Self, ID, MAX>);

    fn progress(
        _: &State<'life, 'd, P, Self, ID, MAX>,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> Progress<'d> {
        Progress::Quiescent
    }

    fn inbound(
        _: &State<'life, 'd, P, Self, ID, MAX>,
        _: conn::Id<'d, ID>,
        _: &Self::ConnState,
        _: Positive,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        app::Inbound::Quiescent
    }
}
