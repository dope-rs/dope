mod events;

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::{Duration, Instant};

use self::events::Events;
use super::SessionApp;
use super::app::ConnApp;
use super::session::Session;
use super::source::{Action, DialKey, Dialer};
use super::state::State;
use crate::DriverContext;
use crate::manifold::Manifold;
use crate::manifold::TypedToken;
use crate::manifold::env::Env;
use crate::manifold::timer::{Ticket, Timer};
use crate::runtime::Idle;
use crate::runtime::profile::RuntimeProfile;
use dope_core::driver::control::ContextControl;
use dope_core::driver::ready::{CompletionWaker, ReadyKey, ReadySlot};
use dope_core::driver::route::Route;
use dope_core::driver::token::{Epoch, SlotIndex, Token};
use dope_core::io::EventKind;
use dope_net::Transport;
use dope_net::link::egress;
use dope_net::link::egress::config::Config;
use dope_net::link::egress::queue::Arena;
use dope_net::link::pool::Pool;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PEND_SHUTDOWN, PendingQueue};
use dope_net::wire::Wire;

type ConnPool<'d, const ID: u8, T, W, C, S> = Pool<'d, ID, T, W, State<C, S>>;

#[pin_project::pin_project(!Unpin)]
pub struct Core<'d, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    route: Route<'d, ID>,
    pub(super) pool: ConnPool<'d, ID, E::Transport, A::Wire, A::Conn, A::Send>,
    egress_arena: egress::queue::Arena<A::Send>,
    pub(super) app: A,
    pub(super) upstreams: S,
    stream: <E::Transport as Transport>::StreamConfig,
    wire_config: <A::Wire as Wire>::InitConfig,
    dirty: PendingQueue,
    backoff_timer: Option<Ticket>,
    liveness_timer: Option<Ticket>,
    timer: Timer<'d, 0>,
    #[pin]
    backoff_slot: ReadySlot<'d>,
    draining: bool,
    _e: PhantomData<E>,
}

impl<'d, const ID: u8, A, S, E> Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    pub fn with_app(
        app: A,
        upstreams: S,
        max_connections: usize,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_app_config(app, upstreams, max_connections, Config::default(), driver)
    }

    pub fn with_app_config(
        app: A,
        mut upstreams: S,
        max_connections: usize,
        egress_config: Config,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        let route = Route::reserve(driver)?;
        let reservation = driver.reserve_outbound(max_connections as u32)?;
        let backoff_sentinel =
            Token::new(ID, SlotIndex::new(max_connections as u32), Epoch::INITIAL);
        let backoff_slot = driver.driver_ref().make_ready_slot(backoff_sentinel);
        upstreams.resize(max_connections);
        let pool = Pool::new(
            max_connections,
            A::max_retained_recv_chunks(max_connections),
            reservation,
            driver,
        )?;
        Ok(Self {
            route,
            pool,
            egress_arena: Arena::with_config(egress_config, max_connections),
            app,
            upstreams,
            stream: <E::Transport as Transport>::StreamConfig::default(),
            wire_config: <<A::Wire as Wire>::InitConfig as Default>::default(),
            dirty: PendingQueue::with_capacity(max_connections),
            backoff_timer: None,
            liveness_timer: None,
            timer: Timer::with_capacity(2, driver.driver_ref()),
            backoff_slot,
            draining: false,
            _e: PhantomData,
        })
    }

    pub fn set_config(&mut self, config: <A::Wire as Wire>::InitConfig) {
        self.wire_config = config;
    }

    fn backoff_key(self: Pin<&Self>) -> ReadyKey<'d> {
        self.project_ref().backoff_slot.key()
    }

    pub fn dial(
        mut self: Pin<&mut Self>,
        addr: <E::Transport as Transport>::Addr,
    ) -> Option<DialKey> {
        let driver = self.as_ref().get_ref().route.driver();
        let this = self.as_mut().project();
        let key = this.upstreams.dial(addr, *this.stream)?;
        driver.activate_ready(self.as_ref().backoff_key());
        Some(key)
    }

    pub fn set_stream_config(
        self: Pin<&mut Self>,
        config: <E::Transport as Transport>::StreamConfig,
    ) {
        *self.project().stream = config;
    }

    pub fn shutdown(&self, conn_id: Token, how: i32) {
        let Some((idx, slot)) = self.pool.by_target(conn_id) else {
            return;
        };
        slot.state.pending.set_shutdown(how);
        self.dirty.mark(idx, &slot.state.pending, PEND_SHUTDOWN);
    }

    pub fn flush(&self, conn_id: Token) {
        let Some((slot_index, slot)) = self.pool.by_target(conn_id) else {
            return;
        };
        self.dirty
            .mark(slot_index, &slot.state.pending, PEND_EGRESS);
    }

    pub fn revive_upstreams(self: Pin<&mut Self>) {
        let ready = self.as_ref().backoff_key();
        let driver = self.as_ref().get_ref().route.driver();
        let this = self.project();
        this.upstreams.revive();
        driver.activate_ready(ready);
    }

    pub fn close(&self, conn_id: Token) {
        let Some((slot_index, slot)) = self.pool.by_target(conn_id) else {
            return;
        };
        self.dirty.mark(slot_index, &slot.state.pending, PEND_CLOSE);
    }

    pub fn state(&self, conn_id: Token) -> Option<&State<A::Conn, A::Send>> {
        let (_, slot) = self.pool.by_target(conn_id)?;
        Some(&slot.state)
    }

    pub fn handler(&self) -> &A {
        &self.app
    }

    fn poll_source(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let mut this = self;
        if *this.as_ref().project_ref().draining {
            return;
        }
        let backoff_fired = {
            let fields = this.as_ref().project_ref();
            fields
                .backoff_timer
                .is_some_and(|ticket| fields.timer.is_fired(ticket))
        };
        if !this.as_ref().project_ref().upstreams.has_pending() && !backoff_fired {
            return;
        }
        let now = driver.turn_now();
        if backoff_fired {
            let fields = this.as_mut().project();
            if let Some(ticket) = fields.backoff_timer.take() {
                fields.timer.cancel(ticket);
            }
        }
        let cap = this.as_ref().project_ref().pool.capacity();
        for _ in 0..cap {
            let action = this.as_mut().project().upstreams.poll_connect(now);
            match action {
                Action::Connect { key } => {
                    let fields = this.as_mut().project();
                    let Some(socket_params) = fields.upstreams.socket_params(key) else {
                        fields.upstreams.connect_outcome(key, false, now);
                        continue;
                    };
                    let submitted = fields.pool.submit_socket_with_state(
                        socket_params,
                        fields.wire_config,
                        |slot| {
                            State::<A::Conn, A::Send>::new(
                                key,
                                slot.raw() as usize,
                                fields.egress_arena,
                            )
                        },
                        driver,
                    );
                    match submitted {
                        Some(slot) => fields.upstreams.bind(key, slot),
                        None => {
                            fields.upstreams.connect_deferred(key, now);
                            break;
                        }
                    }
                }
                Action::Backoff { min_retry_at } => {
                    if this.as_ref().project_ref().backoff_timer.is_none() {
                        this.as_mut().arm_backoff(min_retry_at);
                    }
                    break;
                }
                Action::Idle => break,
            }
        }
    }

    fn arm_backoff(self: Pin<&mut Self>, deadline: Instant) {
        let ready = self.as_ref().backoff_key();
        let wake = CompletionWaker::from_ready(self.as_ref().get_ref().route.driver(), ready);
        let this = self.project();
        if let Some(ticket) = this.backoff_timer.take() {
            this.timer.cancel(ticket);
        }
        *this.backoff_timer = this.timer.try_arm(deadline, wake);
    }

    /// (Re)arm the single inbound-idle deadline. Reuses the connector's backoff
    /// ready slot as the wake target, so firing routes back through `rouse` →
    /// `poll_liveness` exactly like the reconnect-backoff timer. Cancels any
    /// prior arm.
    fn arm_liveness(self: Pin<&mut Self>, deadline: Instant) {
        let ready = self.as_ref().backoff_key();
        let wake = CompletionWaker::from_ready(self.as_ref().get_ref().route.driver(), ready);
        let this = self.project();
        if let Some(ticket) = this.liveness_timer.take() {
            this.timer.cancel(ticket);
        }
        *this.liveness_timer = this.timer.try_arm(deadline, wake);
    }

    /// Earliest `last_recv + timeout` over established, non-retired slots, or
    /// `None` if none qualify. The min the deadline is (re)armed to.
    fn earliest_liveness(self: Pin<&Self>, timeout: Duration) -> Option<Instant> {
        let this = self.project_ref();
        let cap = this.pool.capacity() as u32;
        let mut min: Option<Instant> = None;
        for raw in 0..cap {
            let idx = SlotIndex::new(raw);
            let Some(slot) = this.pool.get(idx) else {
                continue;
            };
            if !slot.state.establish.is_done() || slot.state.retired {
                continue;
            }
            if let Some(seen) = slot.state.last_recv {
                let deadline = seen + timeout;
                min = Some(min.map_or(deadline, |m| m.min(deadline)));
            }
        }
        min
    }

    /// Inbound-idle watchdog, run from `rouse`. Cheap when the deadline has not
    /// fired (one `is_fired` check). On expiry: force a *recoverable* close
    /// (`close_slot` → `upstreams.disconnect` → redial, NOT a permanent `kill`)
    /// for every slot silent past its bound, then re-arm to the next survivor.
    /// A recv that landed since the arm pushed `last_recv` forward, so a slot
    /// that spoke just before this wake is spared and simply re-armed.
    fn poll_liveness(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let fired = {
            let fields = self.as_ref().project_ref();
            fields
                .liveness_timer
                .is_some_and(|ticket| fields.timer.is_fired(ticket))
        };
        if !fired {
            return;
        }
        {
            let this = self.as_mut().project();
            if let Some(ticket) = this.liveness_timer.take() {
                this.timer.cancel(ticket);
            }
        }
        let Some(timeout) = self.as_ref().project_ref().app.inbound_idle_timeout() else {
            return;
        };
        let now = driver.turn_now();
        let cap = self.as_ref().project_ref().pool.capacity() as u32;
        for raw in 0..cap {
            let idx = SlotIndex::new(raw);
            let expired = {
                let this = self.as_ref().project_ref();
                this.pool.get(idx).is_some_and(|slot| {
                    slot.state.establish.is_done()
                        && !slot.state.retired
                        && slot
                            .state
                            .last_recv
                            .is_some_and(|seen| now.duration_since(seen) >= timeout)
                })
            };
            if expired {
                Self::close_slot(self.as_mut(), idx, driver);
            }
        }
        if let Some(deadline) = self.as_ref().earliest_liveness(timeout) {
            self.arm_liveness(deadline);
        }
    }
}

impl<'d, const ID: u8, N, S, E> Core<'d, ID, SessionApp<'d, N, E::Wire>, S, E>
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
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        Self::new_with_egress(
            session,
            upstreams,
            max_connections,
            Config::default(),
            driver,
        )
    }

    pub fn new_with_egress(
        session: N,
        upstreams: S,
        max_connections: usize,
        egress_config: Config,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        let app = SessionApp {
            session,
            _w: PhantomData,
        };
        Self::with_app_config(app, upstreams, max_connections, egress_config, driver)
    }

    pub fn session(&self) -> &N {
        &self.app.session
    }

    pub fn session_mut(self: Pin<&mut Self>) -> &mut N {
        &mut self.project().app.session
    }

    pub fn config(mut self, config: <E::Wire as Wire>::InitConfig) -> Self {
        self.set_config(config);
        self
    }
}

impl<'d, const ID: u8, A, S, E> Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    pub fn dispatch(
        mut self: Pin<&mut Self>,
        ev: dope_core::io::Event,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        match ev.into_kind() {
            EventKind::Recv(token, more, e) => self.as_mut().handle_recv(token, more, e, driver),
            EventKind::Send(token, e) => self.as_mut().handle_send(token, e, driver),
            EventKind::Socket(token, e) => self.as_mut().socket(token, e, driver),
            EventKind::Connect(token, e) => self.as_mut().connect(token, e, driver),
            _ => {}
        }
        self.as_mut().project().pool.flush_rearm(driver);
        self.as_mut().flush_dirty(driver);
    }

    pub fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        {
            let this = self.as_mut().project();
            Pin::new(&*this.timer).pre_park(driver);
            this.app.pre_park();
        }
        self.rouse(driver);
    }

    pub fn activate(mut self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().apply_requests(target);
        self.rouse(driver);
    }

    pub fn idle(self: Pin<&Self>) -> Idle {
        let this = self.project_ref();
        if !this.dirty.is_empty() || (E::Profile::HYBRID_PARK && this.pool.pending_recv_rearm()) {
            return Idle::Busy;
        }
        Pin::new(this.timer).idle().reduce(this.app.idle())
    }

    pub fn shutdown_all(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        {
            let this = self.as_mut().project();
            *this.draining = true;
            if let Some(t) = this.backoff_timer.take() {
                this.timer.cancel(t);
            }
        }
        let cap = self.as_ref().project_ref().pool.capacity() as u32;
        for raw in 0..cap {
            self.as_mut().close_slot(SlotIndex::new(raw), driver);
        }
        self.as_mut().flush_dirty(driver);
        let fields = self.project();
        let mut targets = Vec::new();
        fields.pool.append_io_targets(&mut targets);
        fields.pool.append_outbound_targets(&mut targets);
        let poison = fields.pool.needs_route_poison() || !targets.is_empty();
        if !targets.is_empty() {
            driver.quiesce(&targets);
        }
        fields.route.finish(driver, poison);
    }
}

impl<'d, const ID: u8, A, S, E> Manifold<'d> for Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    const ID: u8 = ID;

    fn dispatch(
        self: Pin<&mut Self>,
        ev: dope_core::io::Event,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.dispatch(ev, driver);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.pre_park(driver);
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.activate(target.into_inner(), driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        self.idle()
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.shutdown_all(driver);
    }
}
