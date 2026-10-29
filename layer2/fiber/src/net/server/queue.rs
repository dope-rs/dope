use std::{convert, pin};

use dope::{
    core::{driver::route::Identity, io::socket, random::Seed},
    net::{
        link::{egress, event::ParkError, pool::recv::input, slot::types::CloseGate},
        wire::{open, receive::Chunk, terminal},
    },
};
use o3::{base::Call, buffer::resident};

use crate::{
    core::driver::{retained, schedule::clock},
    manifold::{
        app::receive::{self, Failure, limits},
        network::listener::{
            conn, handler,
            handler::{failures, retire, send},
        },
    },
    net::{
        error::read,
        port::{self, table::channel::Quiescence},
        server::ListenerPort,
    },
};

#[pin_project::pin_project]
pub(super) struct Queue<'scope, 'w, 'd: 'w, W: input::Execution, F, ID: Identity> {
    pub(super) port: &'scope ListenerPort<'w, 'd, W, ID>,
    pub(super) failed: F,
}

impl<'scope, 'w, 'd: 'w, W: input::Execution, F, ID: Identity> Queue<'scope, 'w, 'd, W, F, ID> {
    fn sync_send<'r>(&self, conn: &mut conn::Ctx<'_, '_, '_, '_, 'r, 'd, ID, W, ()>)
    where
        'd: 'r,
    {
        let id = conn.id();
        let pending = conn.write().pending();
        self.port.connections.channel().sync_send(id, pending);
    }
}

impl<'scope, 'w, 'd: 'w, W, F, ID: Identity> handler::Application<'d, ID>
    for Queue<'scope, 'w, 'd, W, F, ID>
where
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
{
    type Conn = ();
    type Build = convert::Infallible;
    type Wire = W;
    type WireFailure = Self;
    type Retirement = ();
    type Egress = resident::Shared<'d>;
    type Input = receive::Owned;
    type Send = send::Reported<send::Run>;

    fn connection(self: pin::Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(())
    }

    fn build_failed(self: pin::Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn deadline(self: pin::Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn activate<'e, 'a, 'p, 's, 'r>(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'r, 'd, ID, W, (), Self::Egress>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'r, 'd, ID, W, (), Self::Egress>
    where
        'd: 'r,
    {
        self.as_ref().get_ref().sync_send(&mut conn);
        conn
    }

    fn eof<'e>(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, W, (), Self::Egress>,
        end: receive::End,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Eof {
        self.get_mut()
            .port
            .connections
            .channel()
            .end(conn.id(), end);
        match end {
            receive::End::Read => handler::Eof::Hold,
            receive::End::Both => handler::Eof::Close,
        }
    }

    fn close<'e, 'a, 'p, 's, 'r>(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'r, 'd, ID, W, (), Self::Egress>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'r, 'd, ID, W, (), Self::Egress>
    where
        'd: 'r,
    {
        let port = self.get_mut().port;
        match conn.failure() {
            Some(conn::Failure::Receive(error)) => {
                port.connections
                    .channel()
                    .fail_edge(conn.id(), read::Edge::Transport(*error));
            }
            Some(conn::Failure::Submit(error)) => {
                port.connections
                    .channel()
                    .fail_receive(conn.id(), ParkError::Submit(*error));
            }
            Some(conn::Failure::Park(error)) => {
                port.connections.channel().fail_receive(conn.id(), *error);
            }
            _ => {}
        }
        if let Some(cause) = conn.transmit() {
            port.connections
                .channel()
                .fail_edge(conn.id(), read::Edge::Transmit((&cause).into()));
        }
        port.connections.channel().closed(conn.id());
        conn
    }

    fn close_gate(
        self: pin::Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, W, (), Self::Egress>,
    ) -> CloseGate {
        let channel = self.get_ref().port.connections.channel();
        if channel.is_held(conn.id()) {
            return CloseGate::Deferred;
        }
        match channel.quiescence(conn.id()) {
            Quiescence::Pending => CloseGate::Deferred,
            Quiescence::Ready => CloseGate::Immediate,
        }
    }

    fn accept(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, (), Self::Egress>,
    ) -> receive::Outcome {
        let work = conn.work();
        let port = self.get_mut().port;
        port.activate(conn.id(), work)
    }
}

impl<'scope, 'w, 'd: 'w, W: input::Execution + 'd, F: FnMut(open::Error<'w, W>), ID: Identity>
    retire::Policy<'d, Queue<'scope, 'w, 'd, W, F, ID>, ID> for ()
{
    fn select() -> Option<Self> {
        Some(())
    }

    fn retire(
        self,
        app: pin::Pin<&mut Queue<'scope, 'w, 'd, W, F, ID>>,
        conn: conn::Retired<'_, '_, 'd, ID, W, ()>,
    ) {
        app.get_mut()
            .port
            .connections
            .channel()
            .retire_send(conn.id());
    }
}

impl<'scope, 'w, 'd: 'w, W, F, ID: Identity>
    failures::Policy<'w, 'd, Queue<'scope, 'w, 'd, W, F, ID>, ID>
    for Queue<'scope, 'w, 'd, W, F, ID>
where
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
{
    fn open_failed(app: pin::Pin<&mut Self>, cause: open::Error<'w, W>) {
        (app.get_mut().failed)(cause);
    }

    /// Peer-address faults close the socket; the sink accepts only wire failures.
    fn peer() -> Option<impl for<'a> Call<(pin::Pin<&'a mut Self>, socket::Error)>> {
        None::<convert::Infallible>
    }

    fn wire_failed<'e>(
        app: pin::Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, W, (), resident::Shared<'d>>,
        cause: terminal::Error<'w, W>,
    ) {
        app.get_mut()
            .port
            .connections
            .channel()
            .fail_edge(conn.id(), read::Edge::Wire(cause));
    }
}

impl<'scope, 'w, 'd: 'w, W, F, ID: Identity> send::Report<'d, ID>
    for Queue<'scope, 'w, 'd, W, F, ID>
where
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
{
    const ENABLED: bool = true;

    fn released(self: pin::Pin<&mut Self>, release: handler::Release<'d, ID, Self::Egress>) {
        let (id, egress, cause) = release.into_parts();
        let channel = self.get_mut().port.connections.channel();
        match cause {
            handler::Disposition::Sent => channel.sync_send(id, egress::Pending::Absent),
            handler::Disposition::Closed => channel.close_send(id, egress),
        }
    }
}

impl<'scope, 'w, 'd: 'w, W: input::Execution + 'd, F: FnMut(open::Error<'w, W>), ID: Identity>
    send::Drive<'d, ID> for Queue<'scope, 'w, 'd, W, F, ID>
{
    fn send(
        self: pin::Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, (), Self::Egress>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        self.as_ref().get_ref().sync_send(&mut conn);
    }
}

impl<'scope, 'w, 'd: 'w, W: input::Execution + 'd, F: FnMut(open::Error<'w, W>), ID: Identity>
    handler::Owned<'w, 'd, ID> for Queue<'scope, 'w, 'd, W, F, ID>
{
    const RETENTION: limits::Retention = port::Plan::RETENTION;

    fn retention(&self) -> limits::Retention {
        self.port.connections.retention()
    }

    fn chunk(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, W, (), Self::Egress>,
        chunk: Chunk<'w, 'd, W>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        if !matches!(
            self.get_mut()
                .port
                .connections
                .channel()
                .push(conn.id(), chunk, driver.region_token()),
            port::Push::Added
        ) {
            receive::Outcome::Failed(Failure::Contract)
        } else {
            receive::Outcome::Continue
        }
    }
}
