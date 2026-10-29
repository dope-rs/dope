use std::{cell::Cell, convert::Infallible, rc::Rc, task};

use dope_core::{
    driver::{
        self,
        route::{KeyTag, table::conn::Capacity},
        schedule::ready,
        settings,
        settings::{claim, errors::Claim},
        storage,
    },
    io::socket,
};
use dope_manifold::{
    app::Park,
    client::{
        app::{admit::Automatic, connect::Full, failure::Observed, request},
        attempt,
        close::Reason,
        conn,
        session::{
            self, Output, Parser, Plain, Retirement, Scheduling, Target,
            app::drain::Ready,
            codec::mode,
            policy::{self, output},
            response, turns,
        },
        side,
        side::registry,
    },
};
use dope_net::{
    link::{
        egress::{self, data::Inline},
        event,
        pool::change::opening,
    },
    wire,
};
use dope_test::harness::{
    observe::Trace,
    signals::{LocalGateRef, Progress as _},
};
use o3::{
    alloc,
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
    depot::map::direct,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{error, flush},
};

pub mod app;

#[derive(Debug)]
pub enum Error {
    Flush { connected: u32, empty: bool },
    Quarantine(u32),
    Shutdown { pending: bool, done: u32 },
}

#[derive(Debug)]
pub struct Reservation<'d> {
    pub target: conn::Id<'d, KeyTag<0>>,
    pub address: *const u8,
    pub result: Result<(), (registry::Cause<'d>, conn::Id<'d, KeyTag<0>>, *const u8)>,
}

pub struct Intent<'d> {
    pub(crate) target: conn::Id<'d, KeyTag<0>>,
    pub(crate) payload: Shared,
}

impl<'d> registry::Intent<'d> for Intent<'d> {
    type Payload = Shared;

    fn target(&self) -> conn::Id<'d, KeyTag<0>> {
        self.target
    }

    fn into_payload(self) -> Self::Payload {
        self.payload
    }
}

pub struct Storage;

impl settings::Resource for Storage {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, Claim> {
        Ok(needs)
    }
}

impl storage::Plan for Storage {
    type Output<'p, 'd>
        = registry::Registry<'d, Intent<'d>>
    where
        Self: 'd,
        'd: 'p;
}

impl storage::Claimless for Storage {}

impl storage::Build for Storage {
    type Error = alloc::Error;

    type Store<'d>
        = ()
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn try_build<'p, 'd>(
        self,
        _: &'p mut Self::Store<'d>,
        _: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        registry::Registry::try_new(Capacity::fixed::<1>())
    }
}

pub struct Session<'test, 'life, 'd, F, G> {
    pub(crate) create: F,
    pub(crate) ready: G,
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) claims: Rc<Trace<Reservation<'d>, 2>>,

    pub(crate) state: State<'test, 'd>,
    pub(crate) registry: &'life registry::Registry<'d, Intent<'d>>,
    pub(crate) conn: LocalGateRef<'test>,
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>>
    session::Session<'d> for Session<'_, '_, 'd, F, G>
{
    type Codec = codec::Pending;
    type Connect = Full;
    type Requests = request::Disabled;
    type ConnState = ();
    type Failure = Observed;
    type Send = Inline<16>;
    fn codec(&self) -> &Self::Codec {
        &codec::Pending
    }
}

impl<'test, 'life, 'd, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>>
    policy::Compatible<'d, 'd, wire::Identity, KeyTag<0>> for Session<'test, 'life, 'd, F, G>
{
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>> Parser<'d>
    for Session<'_, '_, 'd, F, G>
{
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>> Plain<'d>
    for Session<'_, '_, 'd, F, G>
{
    type Delivery = response::Direct;

    fn reject(
        &mut self,
        _: errors::Truncated,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Direct {
        use dope_manifold::client::close;

        ctx.request(close::Directive::reconnect(close::Reason::Protocol));
        response::Direct::Committed
    }

    fn deliver<'input, R: buffer::Cursor>(
        &mut self,
        head: Infallible,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        match head {}
    }
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>> Output<'d>
    for Session<'_, '_, 'd, F, G>
{
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            let conn = owner.conn.hits();
            let empty = context.sink.is_empty();
            if conn != 1 || !empty {
                owner.audit.faults.record(error::Error::Side(Error::Flush {
                    connected: conn,
                    empty,
                }));
            }
        })
    }

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        conn::Id<'d, KeyTag<0>>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<_>::Ready(|owner: &mut Self| {
            owner.audit.record_sent();
        })
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        use Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>>
    session::Connected<'d> for Session<'_, '_, 'd, F, G>
{
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        context: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        let target = context.connection();
        let ready = (self.ready)(self.registry.ready_target());
        for _ in 0..2 {
            let payload = (self.create)();
            let address = payload.as_ref().as_ptr();
            let result = self
                .registry
                .try_reserve(Intent { target, payload }, ready.into())
                .map_err(|rejected| {
                    let cause = match rejected.cause() {
                        registry::Cause::Stale => registry::Cause::Stale,
                        registry::Cause::Slot(direct::Error::Outside {
                            key,
                            index,
                            capacity,
                        }) => registry::Cause::Slot(direct::Error::Outside {
                            key: *key,
                            index: *index,
                            capacity: *capacity,
                        }),
                        registry::Cause::Slot(direct::Error::Occupied { key, stored }) => {
                            registry::Cause::Slot(direct::Error::Occupied {
                                key: *key,
                                stored: *stored,
                            })
                        }
                    };
                    let (intent, _) = rejected.into_parts();
                    (cause, intent.target, intent.payload.as_ref().as_ptr())
                });
            self.claims.record(Reservation {
                target,
                address,
                result,
            });
        }
        self.state.pending.set(Some(target));
        ready.wake();
        self.conn.hit();
    }
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>>
    session::Failed<'d> for Session<'_, '_, 'd, F, G>
{
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>>
    session::Exhausted<'d> for Session<'_, '_, 'd, F, G>
{
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>> Retirement<'d>
    for Session<'_, '_, 'd, F, G>
{
    type Drain = Ready;

    fn io_fail(
        &mut self,
        error: dope_core::Errno,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        _: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let conn = self.conn.hits();
        if conn != 1 {
            self.audit
                .faults
                .record(error::Error::Side(Error::Quarantine(conn)));
        }
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.state.closed.hit();
    }
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>> Scheduling<'d>
    for Session<'_, '_, 'd, F, G>
{
    fn park() -> Option<impl for<'v, 'w, 'x, 'y> Call<(&'v mut Self, &'w mut Park<'x, 'y, 'd>)>> {
        Some(|(session, _): (&mut Self, &mut Park<'_, '_, 'd>)| {
            let Some(target) = session.state.pending.get() else {
                return;
            };
            let ready = (session.ready)(session.registry.ready_target());
            if session.state.eager {
                if !session.state.armed.replace(true) {
                    ready.wake();
                    return;
                }
                session.registry.abandon(target);
                session.state.pending.set(None);
                session.state.done.hit();
                return;
            }
            match session.registry.poll(target, ready.into()) {
                task::Poll::Pending => {}
                task::Poll::Ready(result) => {
                    session.state.pending.set(None);
                    session.state.events.record(result);
                    session.state.done.hit();
                }
            }
        })
    }

    fn shutdown() -> Option<impl for<'v> Call<&'v mut Self>> {
        Some(|session: &mut Self| {
            let pending = session.state.pending.get().is_some();
            let done = session.state.done.hits();
            if pending || done != 1 {
                session
                    .audit
                    .faults
                    .record(error::Error::Side(Error::Shutdown { pending, done }));
            }
        })
    }
}

impl<'d, F: Fn() -> Shared, G: Fn(Option<ready::Target<'d>>) -> ready::Target<'d>>
    Target<'d, KeyTag<0>, 1> for Session<'_, '_, 'd, F, G>
{
}

pub(crate) struct State<'test, 'd> {
    pub(crate) pending: Cell<Option<conn::Id<'d, KeyTag<0>>>>,
    pub(crate) eager: bool,
    pub(crate) armed: Cell<bool>,
    pub(crate) done: LocalGateRef<'test>,
    pub(crate) closed: LocalGateRef<'test>,
    pub(crate) events: &'test Trace<Result<(), side::Error>, 4>,
}
