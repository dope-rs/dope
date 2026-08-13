mod policy;
pub(in crate::connector) mod scheduling;
mod sealed;
pub(in crate::connector) mod sidecars;
pub(in crate::connector) mod transfer;
pub(in crate::connector) mod transition;

use std::{io, marker, pin};

use dope_core::driver::{
    self,
    lifecycle::routing,
    ops, retained,
    route::{self, table},
    schedule::{self, ready},
};
use dope_net::{
    link::{
        egress, event,
        pool::{self, pending},
    },
    wire,
};
pub(in crate::connector::connection::engine) use sealed::PoolBinding;

use self::{
    scheduling::{deadline, phase, wake},
    transfer::flush,
    transition::{dial, shutdown},
};
use crate::connector::{
    self, app, attempt,
    auxiliary::{self, Ownership as _},
    connection, lifecycle,
};

struct Construction<W> {
    primary_connections: usize,
    egress: egress::Config,
    wire: W,
}

type Pool<'d, const ID: u8, A, E, O> = pool::Outbound<
    'd,
    ID,
    <E as crate::Env>::Transport,
    <A as app::Application<'d, ID>>::Wire,
    connection::State<<A as app::Application<'d, ID>>::Conn, O>,
    <A as app::Application<'d, ID>>::Input,
    <A as app::Application<'d, ID>>::Send,
    { connector::IOV_CAP },
>;

#[pin_project::pin_project(!Unpin)]
pub struct Engine<'d, const ID: u8, A, S, E, X = auxiliary::Disabled>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    pub(in crate::connector) pool:
        Pool<'d, ID, A, E, <X as auxiliary::Mode<'d, A::Send, ID>>::Owner>,
    pub(crate) app: A,
    pub(in crate::connector) controller: S,
    pub(in crate::connector) auxiliary: X,
    pub(in crate::connector) schedule: phase::Schedule<'d, ID>,
    pub(in crate::connector) primary_capacity: u32,
    #[pin]
    pub(in crate::connector) wake: wake::Wake<'d, ID>,
    pub(in crate::connector) _e: marker::PhantomData<E>,
}

impl<'d, const ID: u8, A, S, E, X> Engine<'d, ID, A, S, E, X>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    /// Borrows the protocol application from this address-stable engine.
    pub fn application(self: pin::Pin<&Self>) -> &A {
        self.project_ref().app
    }

    pub(crate) fn controller(self: pin::Pin<&Self>) -> &S {
        self.project_ref().controller
    }

    pub(crate) fn controller_mut(self: pin::Pin<&mut Self>) -> &mut S {
        self.project().controller
    }

    pub(crate) fn connection_capacity(self: pin::Pin<&Self>) -> table::Capacity {
        self.project_ref().pool.inspection().capacity()
    }

    pub(crate) fn wake_controller(self: pin::Pin<&Self>) {
        self.project_ref().wake.target().wake();
    }

    pub(crate) fn attempt_at(
        self: pin::Pin<&Self>,
        lane: route::SlotIndex,
    ) -> Option<attempt::Id<'d, ID>> {
        let this = self.project_ref();
        let key = this.pool.key_at(lane)?;
        this.pool.get(key)?.state.owner.attempt()
    }

    pub(crate) fn request_close_attempt(
        self: pin::Pin<&mut Self>,
        lane: route::SlotIndex,
        attempt: attempt::Id<'d, ID>,
        reason: lifecycle::CloseReason,
    ) -> bool {
        let this = self.project();
        let Some(key) = this.pool.key_at(lane) else {
            return false;
        };
        let Some((slot, handle)) = pending::Mut::of(this.pool).get(key) else {
            return false;
        };
        if slot.state.owner.attempt() != Some(attempt) {
            return false;
        }
        slot.state.request_close(reason);
        handle.mark(pending::Action::Close);
        true
    }
}

impl<'d, const ID: u8, A, S, E, X> Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    pub(crate) fn with_app_configs_mode(
        app: A,
        controller: S,
        auxiliary: X,
        max_connections: usize,
        egress_config: egress::Config,
        wire_config: <A::Wire as wire::Wire>::InitConfig<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_app_configs_on_route(
            app,
            controller,
            auxiliary,
            Construction {
                primary_connections: max_connections,
                egress: egress_config,
                wire: wire_config,
            },
            driver,
            |driver| {
                use dope_core::driver::lifecycle::routing::Route;

                let route = Route::reserve_transaction(driver)?;
                Ok(route.commit())
            },
        )
    }

    fn with_app_configs_on_route(
        app: A,
        mut controller: S,
        auxiliary: X,
        construction: Construction<<A::Wire as wire::Wire>::InitConfig<'d, ID>>,
        driver: &mut driver::Context<'_, 'd>,
        route: impl FnOnce(&mut driver::Context<'_, 'd>) -> io::Result<routing::Route<'d, ID>>,
    ) -> io::Result<Self> {
        use std::io::{Error, ErrorKind};

        use dope_core::driver::route::{Epoch, KeyTag, table::ConnectionCapacity};
        use dope_net::link::pool::Prepared;

        let Construction {
            primary_connections,
            egress,
            wire,
        } = construction;
        let Some(primary_capacity) = ConnectionCapacity::new(primary_connections) else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: connector capacity must be in 1..=2^24-1",
            ));
        };
        let physical_connections = X::physical_capacity(primary_connections).ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "dope: connector capacity overflow")
        })?;
        let Some(physical_capacity) = ConnectionCapacity::new(physical_connections) else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: connector physical capacity must be in 1..=2^24-1",
            ));
        };
        if physical_capacity.get() > ops::Buffers::outbound_capacity(driver) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: connector capacity exceeds driver outbound slots",
            ));
        }
        let capacity = physical_capacity.table();
        let backoff_index = physical_capacity.sentinel();
        let max_retained_recv_chunks =
            <A::Input as app::Policy<'d, ID, A>>::retained_capacity(physical_capacity.get())?;
        attempt::Control::resize(&mut controller, primary_capacity.get())
            .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
        let prepared_pool =
            Prepared::new(capacity, max_retained_recv_chunks, egress, wire, driver)?;
        let schedule = phase::Schedule::try_new(capacity)?;
        let timer = driver.timer();
        let reference = driver.driver_ref();
        let backoff_sentinel =
            route::Space::<KeyTag<ID>>::for_driver(reference).bind(backoff_index, Epoch::INITIAL);
        let backoff_slot = reference
            .ready()
            .make_ready_slot(backoff_sentinel.dispatch())?;
        let binding =
            PoolBinding::new(pool::raw::PreparedOutbound::reserve(prepared_pool, driver)?);
        let route = route(driver)?;
        let pool = binding.bind(route);
        let wake = wake::Wake::new(timer, backoff_slot);
        let mut engine = Self {
            pool,
            app,
            controller,
            auxiliary,
            schedule,
            primary_capacity: primary_capacity.raw(),
            wake,
            _e: marker::PhantomData,
        };
        engine.auxiliary.start(engine.wake.target());
        Ok(engine)
    }
}

use crate::connector::attempt::queue;

impl<'source, 'd, const ID: u8, A, E, X>
    Engine<'d, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E, X>
where
    A: app::Receive<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn with_attempt_source_mode(
        app: A,
        auxiliary: X,
        source: &'source queue::Source<'d, E::Transport, ID>,
        max_connections: usize,
        egress_config: egress::Config,
        wire_config: <A::Wire as wire::Wire>::InitConfig<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_app_configs_on_route(
            app,
            source.control(),
            auxiliary,
            Construction {
                primary_connections: max_connections,
                egress: egress_config,
                wire: wire_config,
            },
            driver,
            |_| {
                source.take_route().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "connector route already bound",
                    )
                })
            },
        )
    }
}

impl<'source, 'd, const ID: u8, A, E>
    Engine<'d, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E>
where
    A: app::Receive<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    #[doc(hidden)]
    pub fn with_attempt_source(
        app: A,
        source: &'source queue::Source<'d, E::Transport, ID>,
        max_connections: usize,
        egress_config: egress::Config,
        wire_config: <A::Wire as wire::Wire>::InitConfig<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_attempt_source_mode(
            app,
            auxiliary::Disabled,
            source,
            max_connections,
            egress_config,
            wire_config,
            driver,
        )
    }
}

impl<'source, 'd, const ID: u8, A, E, C>
    Engine<'d, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E, auxiliary::Enabled<C>>
where
    A: app::Receive<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    C: auxiliary::Control<'d, A::Send, ID>,
{
    #[doc(hidden)]
    pub fn with_attempt_source_and_auxiliary(
        app: A,
        auxiliary: C,
        source: &'source queue::Source<'d, E::Transport, ID>,
        max_connections: usize,
        egress_config: egress::Config,
        wire_config: <A::Wire as wire::Wire>::InitConfig<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_attempt_source_mode(
            app,
            auxiliary::Enabled::new(auxiliary),
            source,
            max_connections,
            egress_config,
            wire_config,
            driver,
        )
    }
}

impl<'d, const ID: u8, A, S, E, X> Engine<'d, ID, A, S, E, X>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    pub(super) fn backoff_key(self: pin::Pin<&Self>) -> ready::Key<'d> {
        self.get_ref().wake.key()
    }
}

impl<'d, const ID: u8, A, S, E, X> Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    pub(super) fn fail_attempt(
        self: pin::Pin<&mut Self>,
        key: attempt::Id<'d, ID>,
        cause: event::ConnectFailure,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let now = driver.turn_now();
        let this = self.project();
        this.app.connect_failed(key, cause, driver);
        this.controller.connect_failed(key, now);
    }
}

impl<'d, const ID: u8, A, S, E, X> Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID>
        + app::Lifecycle<'d, ID>
        + app::RequestSource<'d, ID>
        + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    pub(super) fn rouse(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        for _ in 0..phase::Phase::COUNT {
            let phase = self.as_mut().project().schedule.next_phase();
            match phase {
                phase::Phase::Dirty => {
                    flush::FlushPhase::flush_dirty(self.as_mut(), turn.reborrow(), driver)
                }
                phase::Phase::Cancellations => {
                    sidecars::AuxiliaryPhase::cancel_abandoned(
                        self.as_mut(),
                        turn.reborrow(),
                        driver,
                    );
                    shutdown::ShutdownPhase::flush_cancellations(self.as_mut(), turn.reborrow());
                }
                phase::Phase::Submission => {
                    sidecars::AuxiliaryPhase::poll_requests(self.as_mut(), turn.reborrow(), driver);
                    let retry_ready =
                        dial::DialPhase::submission_retry_ready(self.as_mut(), driver);
                    let deferred = retry_ready
                        && dial::DialPhase::poll_source(self.as_mut(), turn.reborrow(), driver);
                    let rearm_pending = if retry_ready {
                        let this = self.as_mut().project();
                        this.pool
                            .ingress()
                            .flush(turn.reborrow().maintenance(), driver);
                        this.pool.inspection().pending_rearm()
                    } else {
                        false
                    };
                    if rearm_pending && !deferred {
                        dial::DialPhase::defer_submission(
                            self.as_mut(),
                            driver.turn_now(),
                            turn.reborrow(),
                            driver,
                        );
                    } else if retry_ready && !deferred {
                        dial::DialPhase::submission_succeeded(self.as_mut());
                    }
                }
                phase::Phase::Liveness => {
                    deadline::DeadlinePhase::poll_timeouts(self.as_mut(), turn.reborrow(), driver);
                }
            }
            if turn.reborrow().maintenance().remaining() == 0 {
                return;
            }
        }
    }
}

impl<'d, const ID: u8, A, S, E, X> Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    pub(super) fn shutdown_all(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        {
            let mut this = self.as_mut().project();
            this.wake.as_mut().shutdown();
            if matches!(this.schedule.shutdown, phase::Shutdown::Open) {
                this.auxiliary.stop(driver.region_token());
                this.app.shutdown();
                this.schedule.shutdown = phase::Shutdown::Closing(0);
            }
        }
        shutdown::ShutdownPhase::drain_shutdown(self, turn, driver);
    }
}
