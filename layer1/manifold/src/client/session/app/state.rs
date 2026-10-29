use std::marker;

use dope_core::{
    driver::{
        self,
        route::{Identity, table::conn::Capacity},
        schedule,
        schedule::{app::Permit, clock},
    },
    io::socket,
};
use dope_net::{
    link::{
        egress, event,
        pool::{change::opening, recv::input},
    },
    wire::{self, domain, open, receive::Chunk, terminal},
};
use drain::Mode as _;
use o3::{
    base::{Call, cell::region, time::Positive},
    mem::rc,
};

use crate::{
    app::{
        Park, creation,
        receive::{self, limits},
    },
    client::{
        self,
        app::{self, Resumed, defer::Controlled, request::Turn, resume},
        attempt, close, conn,
        session::{
            self,
            app::{admit, drain, ingress, parser, request, retire},
            codec::Codec,
            ingress::Source,
            policy::{Compatible, end, start},
            response::flow::Policy as _,
            turns,
        },
    },
};

pub struct State<'w, 'd: 'w, ID: Identity, N: session::Session<'d, ID>, W: 'd> {
    pub(super) session: N,
    pub(super) ingress:
        ingress::Ingress<'d, <<N as session::Session<'d, ID>>::Codec as Codec>::Staging>,
    wire: marker::PhantomData<(&'w (), *const W)>,
}

impl<'w, 'd: 'w, ID: Identity, N: session::Session<'d, ID>, W: wire::Spec + 'd>
    State<'w, 'd, ID, N, W>
{
    pub fn try_new<S>(
        setup: session::Setup<'d, N, S>,
        limit: Capacity,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Self, creation::Error>
    where
        S: Source<'d>,
    {
        let (inner, limits) = setup.into_parts();
        let plan = Codec::staging(inner.codec());
        let ingress = ingress::Ingress::try_new(limits, limit, plan, driver)?;
        Ok(Self {
            session: inner,
            ingress,
            wire: marker::PhantomData,
        })
    }

    pub const fn session(&self) -> &N {
        &self.session
    }

    pub(crate) fn session_mut(&mut self) -> &mut N {
        &mut self.session
    }
}
impl<'w, 'd: 'w, ID: Identity, N, W> app::Application<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID>,
    W: input::Execution + 'd,
{
    type Conn = session::Connection<'w, 'd, ID, N, W>;
    type Connect = N::Connect;
    type Deferred = Controlled;
    type Failure = N::Failure;
    type Wire = W;
    type Send = N::Send;
    type Input = receive::Owned;
    type Requests = N::Requests;

    fn try_conn(&self) -> Result<Self::Conn, rc::Error> {
        session::Connection::try_new(&self.session, self.ingress.pool(), *self.ingress.budget())
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Receive<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Retirement<'d, ID> + session::Scheduling<'d, ID>,
    W: input::Execution + 'd,
{
    type Continuation = resume::Retained;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, ID> {
        N::Drain::select(&self.session, barrier, driver.region_token())
    }

    fn eof<'e, O>(
        &mut self,
        entry: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _: receive::End,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::Eof {
        let (id, conn, close, ready) = entry.into_io();
        if !matches!(
            self.session.mode(id, &conn.conn_state),
            session::ReceiveMode::Duplex
        ) {
            return app::Eof::Close;
        }
        self.session.eof(&mut turns::Context::new(
            id,
            &mut conn.conn_state,
            egress,
            ready,
            driver.reborrow(),
            close,
        ))
    }

    fn fence<O>(
        &self,
        conn: conn::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::Fence {
        self.session
            .fence(conn.id(), &conn.state().conn_state, driver.region_token())
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Resumable<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Retirement<'d, ID> + session::Scheduling<'d, ID>,
    W: input::Execution + 'd,
{
    fn resident(&self) -> domain::Resident {
        self.ingress.budget().available().into()
    }

    fn wake(&mut self, conn: &mut Self::Conn) -> app::Wake {
        conn.continuation.wake()
    }

    fn inbound<O>(
        &self,
        conn: conn::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        base: Positive,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::Inbound {
        self.session.inbound(
            conn.id(),
            &conn.state().conn_state,
            base,
            driver.region_token(),
        )
    }

    fn resume<'e, 'turn, O>(
        &mut self,
        permit: Permit<'turn, 'd>,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> <Self::Continuation as resume::Mode>::Outcome {
        let (id, conn, close, work, ready) = conn.into_parts();
        let session::Connection {
            ingress: input,
            conn_state: state,
            continuation: flow,
            ..
        } = conn;
        let mut ctx = turns::Context::new(id, state, egress, ready, driver.reborrow(), close);
        let parser = parser::Parser::new(
            &mut self.session,
            input,
            self.ingress.budget(),
            &mut ctx,
            work,
        );
        let next = parser.run_admitted(flow, permit);
        next.finish(&mut ctx)
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Owned<'w, 'd, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Retirement<'d, ID> + session::Scheduling<'d, ID>,
    W: input::Execution + 'd,
{
    const RETENTION: limits::Retention = limits::Retention::new(1, 0);

    fn chunk<'e, O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        chunk: Chunk<'w, 'd, Self::Wire>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> <Self::Continuation as resume::Mode>::Outcome {
        let (id, conn, close, work, ready) = conn.into_parts();
        let session::Connection {
            ingress: input,
            conn_state: state,
            continuation: flow,
            ..
        } = conn;
        if input.try_push(chunk).is_err() {
            return Resumed::Complete(app::ChunkOutcome::failed(receive::Failure::Contract));
        }
        let mut ctx = turns::Context::new(id, state, egress, ready, driver.reborrow(), close);
        let next = parser::Parser::new(
            &mut self.session,
            input,
            self.ingress.budget(),
            &mut ctx,
            work,
        )
        .run_retained(flow);
        next.finish(&mut ctx)
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Lifecycle<'w, 'd, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID>
        + session::Output<'d, ID>
        + session::Retirement<'d, ID>
        + session::Scheduling<'d, ID>
        + Compatible<'w, 'd, W, ID>,
    W: input::Execution + 'd,
{
    fn wire_fail<'e, O>(
        &mut self,
        conn: conn::Io<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        cause: terminal::Error<'w, Self::Wire>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (id, conn, close, ready) = conn.into_parts();
        let mut ctx = turns::Context::new(
            id,
            &mut conn.conn_state,
            egress,
            ready,
            driver.reborrow(),
            close,
        );
        end::Sink::fail(self.session.terminal(), id, cause, ctx.region());
    }

    fn io_fail<'e, O>(
        &mut self,
        conn: conn::Io<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        error: dope_core::Errno,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (id, conn, close, ready) = conn.into_parts();
        let mut ctx = turns::Context::new(
            id,
            &mut conn.conn_state,
            egress,
            ready,
            driver.reborrow(),
            close,
        );
        session::Retirement::io_fail(&mut self.session, error, &mut ctx);
    }

    fn prepare<'e, O>(
        &mut self,
        entry: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (id, conn, close, ready) = entry.into_io();
        N::flush().call((
            &mut self.session,
            &mut turns::Context::new(
                id,
                &mut conn.conn_state,
                egress,
                ready,
                driver.reborrow(),
                close,
            ),
        ));
    }

    fn sent(
        &mut self,
        conn: conn::Id<'d, ID>,
        pending: egress::Pending,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        N::sent().call((&mut self.session, conn, pending, token));
    }

    fn cancel(&mut self, payload: Self::Send, token: &mut region::Token<region::Identity<'d, 1>>) {
        N::cancel().call((&mut self.session, payload, token));
    }

    fn close<'e, O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        reason: close::Reason,
        driver: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        let (id, conn, close, work, ready) = conn.into_retire();
        retire::State::new(self, id, conn, close, work, ready).run(egress, reason, driver)
    }

    fn drained(
        &mut self,
        proof: conn::Retired<'d, ID, Self>,
        conn: &mut Self::Conn,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.session.disconnect(turns::Retired::new(
            proof,
            &mut conn.conn_state,
            driver.reborrow(),
        ));
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Opened<'w, 'd, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + Compatible<'w, 'd, W, ID>,
    W: input::Execution + 'd,
{
    fn open(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: open::Error<'w, W>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        start::Sink::fail(self.session.open(), key, cause, driver);
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Connected<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Connected<'d, ID>,
    W: input::Execution + 'd,
{
    type Activation = admit::Policy<N::Activation>;

    fn connected<'e, O>(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        peer: socket::Addr,
        entry: conn::Established<'_, 'd, ID, W, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (lifetime, conn, close, ready) = entry.into_io();
        let state = conn.activate(self.session.codec());
        self.session.connect(
            key,
            peer,
            turns::Startup::new(lifetime, state, egress, ready, driver.reborrow(), close),
        );
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Peer<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Peer<'d, ID>,
    W: input::Execution + 'd,
{
    type Addr = N::Addr;

    fn connected<'e, O>(
        &mut self,
        peer: Self::Addr,
        entry: conn::Ctx<'_, 'd, ID, W, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (id, conn, close, ready) = entry.into_io();
        let state = conn.activate(self.session.codec());
        self.session.connect(
            peer,
            &mut turns::Context::new(id, state, egress, ready, driver.reborrow(), close),
        );
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Exhausted<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Exhausted<'d, ID>,
    W: input::Execution + 'd,
{
    fn exhaust(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.session.exhaust(key, cause, driver);
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Failed<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Failed<'d, ID> + session::Exhausted<'d, ID>,
    W: input::Execution + 'd,
{
    fn failed(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: event::ConnectFault,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        self.session.failed(key, cause, driver);
    }
}

impl<'w, 'd: 'w, ID: Identity, N: request::Source<'d, ID>, W: input::Execution + 'd>
    app::Requests<'d, ID> for State<'w, 'd, ID, N, W>
{
    fn drain<'e>(&self, turn: &mut Turn<'_, 'e, 'd, Self::Conn, Self::Send, ID>) {
        <N as request::Source<'d, ID>>::drain(&self.session, turn);
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> app::Scheduling<'d, ID> for State<'w, 'd, ID, N, W>
where
    N: session::Parser<'d, ID> + session::Scheduling<'d, ID>,
    W: input::Execution + 'd,
{
    fn park() -> Option<impl for<'a, 'b, 'c, 'e> Call<(&'a mut Self, &'b mut Park<'c, 'e, 'd>)>> {
        let call = N::park()?;
        Some(move |(this, park): (&mut Self, &mut Park<'_, '_, 'd>)| {
            call.call((&mut this.session, park));
        })
    }

    fn shutdown() -> Option<impl for<'a> Call<&'a mut Self>> {
        let call = N::shutdown()?;
        Some(move |this: &mut Self| call.call(&mut this.session))
    }

    fn progress(
        &self,
        region: &region::Token<region::Identity<'d>>,
        clock: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        self.session.progress(region, clock)
    }
}
