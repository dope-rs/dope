use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use dope_core::driver::ready::ReadyKey;
use dope_core::driver::token::Token;
use dope_core::io::Event;
use dope_net::Transport;
use dope_net::link::egress::queue::Queue;
use dope_net::link::egress::storage::Storage as EgressStorage;
use dope_net::link::slot::Slot;
use dope_net::tcp::Tcp;
use dope_net::wire::Wire;
use dope_net::wire::identity::Identity;
use o3::buffer::{RetainBytes, SnapshotBuf};
use o3::cell::RegionToken;
use pin_project::pin_project;

use super::app::{ChunkOutcome, ConnApp, Requests};
use super::codec::Codec;
use super::core::Core;
use super::lifecycle::{Close, Lifecycle};
use super::source::health::Static;
use super::source::{DialKey, Dialer};
use super::state::{IOV_CAP, State};
use crate::DriverContext;
use crate::manifold::Manifold;
use crate::manifold::env::{Bundle, Env};
use crate::manifold::typed::TypedToken;
use crate::runtime::dispatcher::{FinishContext, Idle};
use crate::runtime::profile::Balanced;

const INGRESS_BUF_CAP: usize = 16 * 1024 * 1024;
const INGRESS_INITIAL_CAP: usize = 16 * 1024;

pub struct Ctx<'a, 'pool, 'd, N: Session<'d>> {
    pub conn_id: Token,
    pub state: &'a mut N::ConnState,
    pub sink: Queue<'a, 'pool, IOV_CAP, N::Send>,
    pub region: &'a mut RegionToken<'d>,
}

pub trait Session<'d>: Sized {
    type Codec: Codec;
    type ConnState: Lifecycle;
    type Send: AsRef<[u8]>;

    fn codec(&self) -> &Self::Codec;

    fn activate(&self, token: Token, ready: ReadyKey<'d>, region: &mut RegionToken<'d>) {
        let _ = (token, ready, region);
    }

    fn connect(&mut self, context: &mut Ctx<'_, '_, 'd, Self>);

    fn response(&mut self, head: <Self::Codec as Codec>::Head, context: &mut Ctx<'_, '_, 'd, Self>);

    fn disconnect(&mut self, context: &mut Ctx<'_, '_, 'd, Self>);

    fn flush_trailer(&mut self, context: &mut Ctx<'_, '_, 'd, Self>) {
        let _ = context;
    }

    fn sent(&self, token: Token, sent: usize) {
        let _ = (token, sent);
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
        region: &mut RegionToken<'d>,
    ) -> Requests {
        let _ = (token, push, region);
        Requests::default()
    }

    fn defer_close(
        &self,
        token: Token,
        state: &Self::ConnState,
        region: &mut RegionToken<'d>,
    ) -> bool {
        let _ = (token, region);
        state.defer_close()
    }

    fn is_drained(
        &self,
        token: Token,
        state: &Self::ConnState,
        region: &mut RegionToken<'d>,
    ) -> bool {
        let _ = (token, region);
        state.is_drained()
    }

    fn pre_park(&mut self) {}

    fn idle(&self) -> Idle {
        Idle::Park(None)
    }

    fn inbound_idle_timeout(&self) -> Option<Duration> {
        None
    }
}

pub struct SessionConn<'d, N: Session<'d>> {
    ingress: SnapshotBuf<INGRESS_BUF_CAP>,
    parse_state: <N::Codec as Codec>::ParseState,
    conn_state: N::ConnState,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, N: Session<'d>> Default for SessionConn<'d, N> {
    fn default() -> Self {
        Self {
            ingress: SnapshotBuf::with_capacity_up_to(INGRESS_INITIAL_CAP),
            parse_state: <N::Codec as Codec>::ParseState::default(),
            conn_state: N::ConnState::default(),
            driver: PhantomData,
        }
    }
}

type Marker<'d, W> = (fn() -> W, fn(&'d ()) -> &'d ());

pub struct SessionApp<'d, N: Session<'d>, W: Wire> {
    pub(super) session: N,
    pub(super) wire: PhantomData<Marker<'d, W>>,
}

fn drain_ingress<'pool, 'd, N: Session<'d>>(
    session: &mut N,
    ingress: &mut SnapshotBuf<INGRESS_BUF_CAP>,
    parse_state: &mut <N::Codec as Codec>::ParseState,
    context: &mut Ctx<'_, 'pool, 'd, N>,
) {
    loop {
        let Some(pending) = ingress.snapshot() else {
            break;
        };
        let Some((head, consumed)) = session.codec().parse(parse_state, &pending) else {
            break;
        };
        if consumed == 0 {
            break;
        }
        let Ok(prefix) = ingress.try_consume_prefix(consumed) else {
            break;
        };
        prefix.commit();
        drop(pending);
        session.response(head, context);
        if !matches!(context.state.wants_close(), Close::Keep) {
            break;
        }
    }
}

impl<'d, N: Session<'d>, W: Wire> ConnApp<'d> for SessionApp<'d, N, W> {
    type Conn = SessionConn<'d, N>;
    type Wire = W;
    type Send = N::Send;

    fn chunk<'pool, R: RetainBytes>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        egress: Queue<'_, 'pool, IOV_CAP, Self::Send>,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        if chunk.is_empty() {
            return ChunkOutcome::Ok;
        }
        let conn_id = slot.token();
        let State { conn, .. } = &mut slot.state;
        let SessionConn {
            ingress,
            parse_state,
            conn_state,
            ..
        } = conn;
        if ingress.try_extend_from_slice(chunk.as_slice()).is_err() {
            return ChunkOutcome::Overrun;
        }
        let mut context = Ctx {
            conn_id,
            state: conn_state,
            sink: egress,
            region: driver.region_token(),
        };
        drain_ingress(&mut self.session, ingress, parse_state, &mut context);
        match conn_state.wants_close() {
            Close::Keep => ChunkOutcome::Ok,
            Close::Reconnect => ChunkOutcome::CloseReconnect,
            Close::Permanent => ChunkOutcome::ClosePermanent,
        }
    }

    fn connected<'pool>(
        &mut self,
        _key: DialKey,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        egress: Queue<'_, 'pool, IOV_CAP, Self::Send>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let conn_id = slot.token();
        self.session
            .activate(conn_id, slot.ready_key(), driver.region_token());
        let State { conn, .. } = &mut slot.state;
        self.session.connect(&mut Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
            region: driver.region_token(),
        });
    }

    fn before_send<'pool>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        egress: Queue<'_, 'pool, IOV_CAP, Self::Send>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let conn_id = slot.token();
        let State { conn, .. } = &mut slot.state;
        self.session.flush_trailer(&mut Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
            region: driver.region_token(),
        });
    }

    fn send<'pool>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        _egress: Queue<'_, 'pool, IOV_CAP, Self::Send>,
        sent: usize,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.session.sent(slot.token(), sent);
    }

    fn close<'pool>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        egress: Queue<'_, 'pool, IOV_CAP, Self::Send>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let conn_id = slot.token();
        let State { conn, .. } = &mut slot.state;
        let mut context = Ctx {
            conn_id,
            state: &mut conn.conn_state,
            sink: egress,
            region: driver.region_token(),
        };
        drain_ingress(
            &mut self.session,
            &mut conn.ingress,
            &mut conn.parse_state,
            &mut context,
        );
        if matches!(context.state.wants_close(), Close::Keep) {
            let remaining = conn.ingress.snapshot().unwrap_or_default();
            if let Some(head) = self
                .session
                .codec()
                .finish(&mut conn.parse_state, remaining)
            {
                self.session.response(head, &mut context);
            }
        }
        self.session.disconnect(&mut context);
    }

    fn defer_close(
        &self,
        slot: &Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.session.defer_close(
            slot.token(),
            &slot.state.conn.conn_state,
            driver.region_token(),
        )
    }

    fn is_drained(
        &self,
        slot: &Slot<'d, Self::Wire, State<Self::Conn, Self::Send>>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        self.session.is_drained(
            slot.token(),
            &slot.state.conn.conn_state,
            driver.region_token(),
        )
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Requests {
        self.session
            .drain_requests(token, push, driver.region_token())
    }

    fn pre_park(&mut self) {
        self.session.pre_park();
    }

    fn idle(&self) -> Idle {
        self.session.idle()
    }

    fn inbound_idle_timeout(&self) -> Option<Duration> {
        self.session.inbound_idle_timeout()
    }
}

type SessionCore<'d, const ID: u8, N, S, E> =
    Core<'d, 'd, ID, SessionApp<'d, N, <E as Env>::Wire>, S, E>;

#[repr(transparent)]
#[pin_project]
pub struct Connector<'d, const ID: u8, N, S = Static<Tcp>, E = Bundle<Tcp, Identity, Balanced>>
where
    N: Session<'d>,
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport,
{
    #[pin]
    core: SessionCore<'d, ID, N, S, E>,
}

impl<'d, const ID: u8, N, S, E> Connector<'d, ID, N, S, E>
where
    N: Session<'d>,
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport,
{
    pub fn new(
        session: N,
        upstreams: S,
        max_connections: usize,
        egress_storage: &'d EgressStorage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <E::Wire as Wire>::InitConfig<'d>: Default,
    {
        Core::new(session, upstreams, max_connections, egress_storage, driver)
            .map(|core| Self { core })
    }

    pub fn new_with_wire_config(
        session: N,
        upstreams: S,
        max_connections: usize,
        wire_config: <E::Wire as Wire>::InitConfig<'d>,
        egress_storage: &'d EgressStorage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        Core::new_with_wire_config(
            session,
            upstreams,
            max_connections,
            wire_config,
            egress_storage,
            driver,
        )
        .map(|core| Self { core })
    }

    pub fn session(&self) -> &N {
        self.core.session()
    }

    pub fn session_mut(self: Pin<&mut Self>) -> &mut N {
        self.project().core.session_mut()
    }

    pub fn revive_upstreams(self: Pin<&mut Self>) {
        self.project().core.revive_upstreams();
    }
}

impl<'d, const ID: u8, N, S, E> Manifold<'d> for Connector<'d, ID, N, S, E>
where
    N: Session<'d>,
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport,
{
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, event: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        self.project().core.dispatch(event, driver);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.project().core.pre_park(driver);
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.project().core.activate(target.into_inner(), driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        self.project_ref().core.idle()
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.project().core.shutdown_all(driver);
    }

    fn finish(self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        self.project().core.finish(context);
    }
}
