use dope::{
    core::driver::route::Identity,
    net::{
        self,
        link::{
            egress, event,
            pool::{change::opening, recv::input},
        },
        wire::{open, receive::Chunk, terminal},
    },
};
use o3::{
    base::{Call, cell::region},
    buffer::resident,
    mem::rc,
};

use crate::{
    context::wake,
    core::{
        driver::{
            self,
            schedule::{self, clock},
        },
        io::socket,
    },
    manifold::{
        app::{
            Park,
            receive::{self, limits},
        },
        client::{
            self,
            app::{
                self, admit::Automatic, connect::Full, defer::Controlled, failure::Observed,
                request, resume,
            },
            attempt, close, conn,
        },
    },
    net::{
        client::{Outcome, Port, State},
        error::read,
        port::{self, table::channel::Quiescence},
    },
};

/// Application capability that binds a client machine to its owning port.
pub struct App<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity> {
    pub(super) port: &'scope Port<'w, 'd, T, W, ID>,
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Application<'d, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    type Conn = State;
    type Connect = Full;
    type Deferred = Controlled;
    type Failure = Observed;
    type Wire = W;
    type Send = resident::Shared<'d>;
    type Input = receive::Owned;
    type Requests = request::Enabled;

    fn try_conn(&self) -> Result<Self::Conn, rc::Error> {
        Ok(State)
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Receive<'d, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, ID> {
        self.port
            .conns
            .channel()
            .sync_send(barrier.connection(), egress::Pending::Absent);
        match self.port.conns.channel().quiescence(barrier.connection()) {
            Quiescence::Ready => barrier.ready(),
            Quiescence::Pending => barrier.pending(),
        }
    }

    fn eof<'e, O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        end: receive::End,
        _: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Eof {
        self.port.conns.channel().end(conn.id(), end);
        match end {
            receive::End::Read => app::Eof::Retain,
            receive::End::Both => app::Eof::Close,
        }
    }

    fn fence<O>(
        &self,
        conn: conn::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Fence {
        if self.port.conns.channel().is_held(conn.id()) {
            app::Fence::Pending
        } else {
            app::Fence::Ready
        }
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Owned<'w, 'd, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    const RETENTION: limits::Retention = port::Plan::RETENTION;

    fn retention(&self) -> limits::Retention {
        self.port.conns.retention()
    }

    fn chunk<O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _out: egress::Queue<'_, '_, 'd, { client::IOV_CAP }, Self::Send>,
        chunk: Chunk<'w, 'd, W>,
        drv: &mut driver::Context<'_, 'd>,
    ) -> app::ChunkOutcome {
        if !matches!(
            self.port
                .conns
                .channel()
                .push(conn.id(), chunk, drv.region_token()),
            port::Push::Added
        ) {
            app::ChunkOutcome::failed(receive::Failure::Contract)
        } else {
            app::ChunkOutcome::ok()
        }
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Opened<'w, 'd, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    fn open(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: open::Error<'w, W>,
        _driver: &mut driver::Context<'_, 'd>,
    ) {
        self.port
            .complete(key, Outcome::Open(opening::Failure::Wire(cause)));
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Connected<'d, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    type Activation = Automatic;

    fn connected<O>(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        conn: conn::Established<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _: egress::Queue<'_, '_, 'd, { client::IOV_CAP }, Self::Send>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.port
            .connected(key, conn.id(), wake::RootWaker::from(conn.wake_target()));
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Exhausted<'d, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    fn exhaust(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.port.complete(key, Outcome::Open(cause.into()));
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Failed<'d, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    fn failed(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: event::ConnectFault,
        _driver: &mut driver::Context<'_, 'd>,
    ) {
        self.port.complete(key, Outcome::Transport(cause));
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Requests<'d, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    fn drain(&self, turn: &mut request::Turn<'_, '_, 'd, Self::Conn, resident::Shared<'d>, ID>) {
        let conn = turn.connection();
        let (mut reqs, _) = turn.requests();
        self.port.conns.drain(conn, &mut reqs);
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Scheduling<'d, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    fn park() -> Option<impl for<'a, 'b, 'c, 'e> Call<(&'a mut Self, &'b mut Park<'c, 'e, 'd>)>> {
        Some(|(this, park): (&mut Self, &mut Park<'_, '_, 'd>)| {
            let work = park.work();
            this.port.conns.pre_park(work, &mut park.regions().0);
        })
    }

    fn shutdown() -> Option<impl for<'a> Call<&'a mut Self>> {
        Some(|this: &mut Self| this.port.conns.shutdown())
    }

    fn progress(
        &self,
        _region: &region::Token<region::Identity<'d>>,
        _clock: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        self.port.conns.progress()
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: input::Execution + 'd, ID: Identity>
    app::Lifecycle<'w, 'd, ID> for App<'scope, 'w, 'd, T, W, ID>
{
    fn prepare<O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        out: egress::Queue<'_, '_, 'd, { client::IOV_CAP }, Self::Send>,
        _drv: &mut driver::Context<'_, 'd>,
    ) {
        let pending = out.pending();
        self.port.conns.channel().sync_send(conn.id(), pending);
    }

    fn sent(
        &mut self,
        conn: conn::Id<'d, ID>,
        pending: egress::Pending,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        self.port.conns.channel().sync_send(conn, pending);
    }

    /// Reclaims the cancelled buffer's retained owner before retirement continues.
    fn cancel(&mut self, payload: Self::Send, _: &mut region::Token<region::Identity<'d, 1>>) {
        drop(payload);
    }

    fn close<O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _out: egress::Queue<'_, '_, 'd, { client::IOV_CAP }, Self::Send>,
        reason: close::Reason,
        _driver: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        if let Some(cause) = conn.transmit() {
            self.port
                .conns
                .channel()
                .fail_edge(conn.id(), read::Edge::Transmit(cause.into()));
        }
        if let close::Reason::Receive(error) = reason {
            self.port.conns.channel().fail_receive(conn.id(), error);
        }
        self.port.conns.channel().end(conn.id(), receive::End::Both);
        app::CloseOutcome::Complete(close::Directive::reconnect(reason))
    }

    fn wire_fail<O>(
        &mut self,
        conn: conn::Io<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _out: egress::Queue<'_, '_, 'd, { client::IOV_CAP }, Self::Send>,
        cause: terminal::Error<'w, W>,
        _driver: &mut driver::Context<'_, 'd>,
    ) {
        self.port
            .conns
            .channel()
            .fail_edge(conn.id(), read::Edge::Wire(cause));
    }

    fn io_fail<O>(
        &mut self,
        conn: conn::Io<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _out: egress::Queue<'_, '_, 'd, { client::IOV_CAP }, Self::Send>,
        error: crate::core::Errno,
        _drv: &mut driver::Context<'_, 'd>,
    ) {
        self.port
            .conns
            .channel()
            .fail_edge(conn.id(), read::Edge::Transport(error));
    }

    fn drained(
        &mut self,
        proof: conn::Retired<'d, ID, Self>,
        _state: &mut Self::Conn,
        _drv: &mut driver::Context<'_, 'd>,
    ) {
        if let Some(cause) = proof.transmit() {
            self.port
                .conns
                .channel()
                .fail_edge(proof.connection(), read::Edge::Transmit(cause.into()));
        }
        self.port.conns.channel().closed(proof.connection());
    }
}
