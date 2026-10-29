use codec::mode;
use codec::storage;
use dope_core::{
    driver::{self, route::Identity, schedule, schedule::clock},
    io::socket,
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire,
};
use dope_runtime::client;
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer::{self, hold, resident},
};
use policy::{admit::Register, end, start};
use response::{flow::Begin, work};

use crate::{
    app::Park,
    client::{
        app::{self, request},
        attempt, close, conn,
        session::{
            self,
            app::{admit, drain, request::Source},
            codec,
            codec::staging,
            policy, protocol, response,
            turns::{self, Retired, Retirement, Startup},
        },
    },
    service::bindings::{
        endpoint::{Provider, State},
        failures::{self, External, Root},
    },
};

type Token<'d, const ID: u8 = 0> = region::Token<region::Identity<'d, ID>>;
type Context<'a, 'e, 'd, N, ID> = turns::Context<
    'a,
    'e,
    'd,
    <N as session::Session<'d, ID>>::ConnState,
    <N as session::Session<'d, ID>>::Send,
    ID,
>;

pub struct Bound<'life, 'd: 'life, P: ?Sized, L, ID: Identity, const MAX: usize, F = Root> {
    endpoint: State<'life, 'd, P, L, ID, MAX>,
    failure: F,
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize>
    Bound<'life, 'd, P, L, ID, MAX, F>
{
    pub const fn root(&self) -> &'life P {
        self.endpoint.root
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, ID: Identity, const MAX: usize>
    Bound<'life, 'd, P, L, ID, MAX>
{
    pub const fn bind(root: &'life P, logic: L) -> Self {
        Self {
            endpoint: State::new(root, logic),
            failure: Root,
        }
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize>
    Bound<'life, 'd, P, L, ID, MAX, External<F>>
{
    pub const fn bind_with(root: &'life P, logic: L, fail: F) -> Self {
        Self {
            endpoint: State::new(root, logic),
            failure: External(fail),
        }
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> client::Provider<'d>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    P: Provider<'life, 'd, ID, MAX>,
{
    type Client<'app>
        = P::Client<'app, Self>
    where
        Self: 'app,
        'd: 'app;
    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (provider, bound) = context.into_parts();
        <P as Provider<'life, 'd, ID, MAX>>::provide(provider.get_ref().endpoint.root, bound)
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Session<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: protocol::Spec<'life, 'd, P, ID, MAX>,
{
    type Codec = L::Codec;
    type Connect = L::Connect;
    type ConnState = L::ConnState;
    type Failure = L::Failure;
    type Requests = L::Requests;
    type Send = L::Send;

    fn codec(&self) -> &Self::Codec {
        L::codec(&self.endpoint)
    }

    fn mode(&self, conn: conn::Id<'d, ID>, state: &Self::ConnState) -> session::ReceiveMode {
        L::mode(&self.endpoint, conn, state)
    }

    fn duplex<'id, 'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        input: codec::Input<'id, 'input, 'd, R>,
        ctx: &mut Context<'_, 'e, 'd, Self, ID>,
    ) -> session::Duplex<'id>
    where
        'd: 'input,
    {
        L::duplex(&mut self.endpoint, input, ctx)
    }

    fn finish<'e>(
        &mut self,
        reason: close::Reason,
        ctx: &mut Context<'_, 'e, 'd, Self, ID>,
    ) -> session::Settlement {
        L::finish(&mut self.endpoint, reason, ctx)
    }

    fn eof<'e>(&mut self, ctx: &mut Context<'_, 'e, 'd, Self, ID>) -> app::Eof {
        L::eof(&mut self.endpoint, ctx)
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Parser<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: protocol::Spec<'life, 'd, P, ID, MAX>,
    L::Mode: mode::Policy<'d, ID, Self, L::Response>,
    L::Response: response::Mode<'d, ID, Self, Flow: Begin<'d, ID, Self, L::Response>>,
{
    type Mode = L::Mode;
    type Response = L::Response;
}
impl<'life, 'd: 'life, P: ?Sized, L, F, Q, ID: Identity, const MAX: usize>
    session::Resume<'d, Q, Q, ID> for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: protocol::Spec<'life, 'd, P, ID, MAX>,
    L::Response: response::Plan<Prepared = Q, Pending = Q>,
    Q: response::Action<'life, 'd, P, L, ID, MAX>,
{
    fn commit<'e, A: work::Admission>(
        &mut self,
        item: response::Committed<Q>,
        work: &mut A,
        ctx: &mut Context<'_, 'e, 'd, Self, ID>,
    ) -> response::Settlement<Q> {
        Q::commit(item, &mut self.endpoint, work, ctx)
    }

    fn settle<'e, A: work::Admission>(
        &mut self,
        item: response::Item<Q>,
        work: &mut A,
        ctx: &mut Context<'_, 'e, 'd, Self, ID>,
    ) -> response::Settlement<Q> {
        Q::settle(item, &mut self.endpoint, work, ctx)
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Plain<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: protocol::Plain<'life, 'd, P, ID, MAX>,
    L::Codec: codec::Stateless,
{
    type Delivery = <L::Response as response::Plan>::Delivery;

    fn deliver<'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        head: <Self::Codec as codec::Codec>::Head<'input, 'd, R>,
        ctx: &mut Context<'_, 'e, 'd, Self, ID>,
    ) -> <L::Response as response::Plan>::Delivery
    where
        'd: 'input,
    {
        protocol::Plain::deliver(&mut self.endpoint, head, ctx)
    }

    fn reject<'e>(
        &mut self,
        error: <Self::Codec as codec::Codec>::Error,
        ctx: &mut Context<'_, 'e, 'd, Self, ID>,
    ) -> Self::Delivery {
        protocol::Plain::reject(&mut self.endpoint, error, ctx)
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Stored<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: protocol::Stored<'life, 'd, P, ID, MAX>,
    L::Codec: codec::Stateful,
{
    type Delivery = <L::Response as response::Plan>::Delivery;

    fn blocked(
        &self,
        conn: conn::Id<'d, ID>,
        state: &mut <Self::Codec as codec::Stateful>::State<'d>,
    ) -> session::Blocking {
        protocol::Stored::blocked(&self.endpoint, conn, state)
    }

    fn decode<S>(
        &mut self,
        conn: conn::Id<'d, ID>,
        storage: &mut <Self::Codec as codec::Stateful>::Storage<S>,
        state: &mut <Self::Codec as codec::Stateful>::State<'d>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut Context<'_, '_, 'd, Self, ID>,
    ) -> codec::Step<Self::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        protocol::Stored::decode(&mut self.endpoint, conn, storage, state, budget, limit, ctx)
    }

    fn finish<'slot, 'id, S>(
        &mut self,
        input: storage::Owner<
            'slot,
            'id,
            <Self::Codec as codec::Stateful>::Storage<S>,
            <Self::Codec as codec::Stateful>::State<'d>,
        >,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut Context<'_, '_, 'd, Self, ID>,
    ) -> (
        storage::Owner<
            'slot,
            'id,
            <Self::Codec as codec::Stateful>::Storage<S>,
            <Self::Codec as codec::Stateful>::State<'d>,
        >,
        Result<codec::Finish<Self::Delivery>, Self::Delivery>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        protocol::Stored::finish(&mut self.endpoint, input, budget, limit, ctx)
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> Source<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Requests<'life, 'd, P, ID, MAX>,
    Self: session::Parser<'d, ID>
        + session::Session<'d, ID, Requests = request::Enabled, Send = L::Send>,
{
    fn drain<'w, 'e, 'turn, W: wire::Spec + 'd>(
        &self,
        turn: &mut request::Turn<
            'turn,
            'e,
            'd,
            session::Connection<'w, 'd, ID, Self, W>,
            Self::Send,
            ID,
        >,
    ) where
        'd: 'w,
    {
        let (mut requests, token) = turn.requests();
        policy::Requests::drain(&self.endpoint, &mut requests, token);
    }
}
impl<'life, 'w, 'd: 'life, P: ?Sized, L, W, F, ID: Identity, const MAX: usize>
    policy::Compatible<'w, 'd, W, ID> for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: protocol::Spec<'life, 'd, P, ID, MAX>,
    W: wire::Spec + 'w,
    F: failures::Source<'life, P>,
    F::Sink: start::Sink<'w, 'd, W, ID> + end::Sink<'w, 'd, W, ID>,
{
    type Open = F::Sink;
    type Terminal = F::Sink;

    fn open(&self) -> &Self::Open {
        self.failure.get(self.endpoint.root)
    }

    fn terminal(&self) -> &Self::Terminal {
        self.failure.get(self.endpoint.root)
    }
}

impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Output<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Output<'life, 'd, P, ID, MAX>,
{
    fn flush()
    -> impl for<'a, 'c, 't, 'e> Call<(&'a mut Self, &'c mut Context<'t, 'e, 'd, Self, ID>)> {
        |(owner, ctx): (&mut Self, &mut Context<'_, '_, 'd, Self, ID>)| {
            L::flush().call((&mut owner.endpoint, ctx));
        }
    }

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        conn::Id<'d, ID>,
        egress::Pending,
        &'t mut Token<'d, 1>,
    )> {
        |(owner, conn, pending, token): (
            &mut Self,
            conn::Id<'d, ID>,
            egress::Pending,
            &mut Token<'d, 1>,
        )| {
            L::sent().call((&mut owner.endpoint, conn, pending, token));
        }
    }

    fn cancel() -> impl for<'a, 't> Call<(&'a mut Self, Self::Send, &'t mut Token<'d, 1>)> {
        |(owner, payload, token): (&mut Self, Self::Send, &mut Token<'d, 1>)| {
            L::cancel().call((&owner.endpoint, payload, token));
        }
    }
}

impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> admit::Register<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Lifecycle<'life, 'd, P, ID, MAX> + Register<'life, 'd, P, ID, MAX>,
{
    fn register(&self, entry: &conn::Activation<'_, 'd, ID>) -> app::Admission {
        L::register(&self.endpoint, entry)
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Retirement<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Lifecycle<'life, 'd, P, ID, MAX>,
    L::Drain: drain::Mode<'d, ID, Self>,
{
    type Drain = L::Drain;
    fn io_fail<'e>(&mut self, error: dope_core::Errno, ctx: &mut Context<'_, 'e, 'd, Self, ID>) {
        L::io_fail(&mut self.endpoint, error, ctx);
    }
    fn quarantine(
        &self,
        conn: conn::Id<'d, ID>,
        state: &mut Self::ConnState,
        reason: close::Reason,
        region: &mut (Token<'d>, Token<'d, 1>),
    ) {
        L::quarantine(&self.endpoint, conn, state, reason, region);
    }
    fn clear_send<'turn>(
        &self,
        conn: conn::Id<'d, ID>,
        ctx: &mut Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        L::clear_send(&self.endpoint, conn, ctx)
    }
    fn clear_recv<'turn>(
        &self,
        conn: conn::Id<'d, ID>,
        reason: close::Reason,
        ctx: &mut Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        L::clear_recv(&self.endpoint, conn, reason, ctx)
    }
    fn fence(
        &self,
        conn: conn::Id<'d, ID>,
        state: &Self::ConnState,
        region: &mut Token<'d>,
    ) -> app::Fence {
        L::fence(&self.endpoint, conn, state, region)
    }
    fn policy(
        &self,
        conn: conn::Id<'d, ID>,
        state: &Self::ConnState,
        reason: close::Reason,
        region: &mut Token<'d>,
    ) -> close::Policy {
        L::policy(&self.endpoint, conn, state, reason, region)
    }
    fn disconnect(&mut self, ctx: Retired<'_, 'd, Self::ConnState, ID, Self>) {
        L::disconnect(&mut self.endpoint, ctx.into_policy());
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> drain::Quiescence<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Quiescence<'life, 'd, P, ID, MAX>,
{
    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        region: &mut Token<'d>,
    ) -> app::Drain<'a, 'd, ID> {
        L::quiesce(&self.endpoint, barrier, region)
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Connected<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Connected<'life, 'd, P, ID, MAX>,
    Self: admit::Register<'d, ID, L::Activation>,
{
    type Activation = L::Activation;

    fn connect<'e>(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        peer: socket::Addr,
        ctx: Startup<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    ) {
        L::connect(&mut self.endpoint, key, peer, ctx);
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Peer<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Peer<'life, 'd, P, ID, MAX>,
{
    type Addr = L::Addr;
    fn connect<'e>(&mut self, peer: Self::Addr, ctx: &mut Context<'_, 'e, 'd, Self, ID>) {
        L::connect(&mut self.endpoint, peer, ctx);
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Exhausted<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Exhausted<'life, 'd, P, ID, MAX>,
{
    fn exhaust(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        L::exhaust(&mut self.endpoint, key, cause, driver);
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Failed<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Failed<'life, 'd, P, ID, MAX>,
{
    fn failed(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: event::ConnectFault,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        L::failed(&mut self.endpoint, key, cause, driver);
    }
}
impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize> session::Scheduling<'d, ID>
    for Bound<'life, 'd, P, L, ID, MAX, F>
where
    L: policy::Scheduling<'life, 'd, P, ID, MAX>,
{
    fn park() -> Option<impl for<'v, 'w, 'x, 'y> Call<(&'v mut Self, &'w mut Park<'x, 'y, 'd>)>> {
        Some(|(session, park): (&mut Self, &mut Park<'_, '_, 'd>)| {
            L::park(&mut session.endpoint, park.work(), park.regions())
        })
    }

    fn shutdown() -> Option<impl for<'v> Call<&'v mut Self>> {
        Some(|session: &mut Self| L::shutdown(&mut session.endpoint))
    }

    fn progress(&self, region: &Token<'d>, clock: clock::Clock<'_, 'd>) -> schedule::Progress<'d> {
        L::progress(&self.endpoint, region, clock)
    }

    fn inbound(
        &self,
        conn: conn::Id<'d, ID>,
        state: &Self::ConnState,
        base: Positive,
        region: &mut Token<'d>,
    ) -> app::Inbound {
        L::inbound(&self.endpoint, conn, state, base, region)
    }
}
