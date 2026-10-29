use dope_core::driver::{
    self,
    route::{Identity, indices, lease, table::conn::Capacity},
};
use dope_net::link::{egress, egress::reserve::demand, pool, pool::change};
use o3::base::time::Positive;

use crate::{
    app::{creation, env},
    client::{
        self, app, attempt,
        attempt::queue,
        conn,
        conn::engine::{
            self,
            drive::{cycle, phase},
            liveness, retain,
            shutdown::state,
        },
        side,
        side::startup,
    },
};

pub struct Startup<W, A, S, X, P = ()> {
    pub(crate) aux: X,
    pub(crate) ctl: S,
    pub(crate) app: A,
    pub(crate) capacities: side::Capacities,
    pub(crate) egress: egress::Config,
    pub(crate) wire: W,
    pub(crate) connection_lifetime: Option<Positive>,
    pub(crate) source: P,
}

pub(super) enum RoutePlan<'source, 'd, T: dope_net::Transport, ID: Identity> {
    Owned,
    Attempts(&'source queue::Source<'d, T, ID>),
}

impl<'source, 'd, T: dope_net::Transport, ID: Identity + 'd> RoutePlan<'source, 'd, T, ID> {
    fn prepare(
        self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<lease::Claim<'d, ID>, creation::Error> {
        match self {
            Self::Owned => driver
                .storage
                .reborrow()
                .try_reserve::<ID>()
                .map_err(creation::Error::Route),
            Self::Attempts(source) => match source.take_route() {
                Some(claim) => Ok(claim),
                None => Err(creation::Error::Config(
                    crate::ConfigError::ConnectorRouteUnavailable,
                )),
            },
        }
    }
}

impl<R, A, S, X, P> Startup<R, A, S, X, P> {
    pub(super) fn build<'source, 'w, 'd: 'w, ID: Identity + 'd, E, C, O>(
        self,
        io: X::Transport<<E::Transport as dope_net::Transport>::Prepared>,
        total: Capacity,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        route: RoutePlan<'source, 'd, E::Transport, ID>,
        build: impl for<'p> FnOnce(engine::Machine<'p, 'w, 'd, ID, A, S, E, X>, &mut C) -> O,
    ) -> Result<O, creation::Error>
    where
        A: app::Receive<'d, ID>,
        (A::Input, A::Continuation): retain::Policy<'w, 'd, ID, A>,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: startup::Startup<'d, ID>,
        R: change::Prepare<'w, 'd, ID, A::Wire>,
    {
        use dope_core::driver::route::Epoch;

        let Self {
            capacities: caps,
            egress,
            wire,
            connection_lifetime: ttl,
            ..
        } = self;
        let main = caps.primary();
        let count = total.entries().get();
        let capacity = context(owner).driver_ref().outbound_cap().count().get();
        if count > capacity {
            return Err(creation::Error::Config(
                crate::ConfigError::ConnectorOutboundCapacity {
                    requested: count,
                    available: capacity,
                },
            ));
        }
        let edge = indices::Edge::from(total);
        let (app, ctl, retain, bytes) =
            <(A::Input, A::Continuation) as retain::Policy<'w, 'd, ID, A>>::prepare::<
                S,
                E::Transport,
            >(self.app, self.ctl, main)?;
        let setup = demand::Fallible::prepare(egress, main);
        change::try_prepare::<
            ID,
            E::Transport,
            A::Wire,
            conn::State<'d, A::Conn, side::Primary<'d, ID>>,
            A::Input,
            A::Send,
            { client::IOV_CAP },
            _,
            _,
            _,
            _,
        >(
            retain,
            bytes,
            setup,
            wire,
            owner,
            &context,
            |base, owner| {
                let machine = {
                    let mut driver = context(owner);
                    let timer = driver.driver_ref().timer();
                    let stop = state::State::new();
                    let handle = driver.driver_ref();
                    let target = handle.targets::<ID>().bind(edge.slot(), Epoch::INITIAL);
                    let ready = handle
                        .ready()
                        .make_slot(target.dispatch())
                        .map_err(creation::Error::Ready)?;
                    let drive = cycle::Cycle::new(timer, ready, phase::Phase::empty())
                        .map_err(creation::Error::Timer)?;
                    let bound = pool::raw::Prepared::try_reserve(base, total.into(), &driver)?;
                    let bound = bound.try_flights(driver.storage.reborrow())?;
                    let route = route.prepare(&mut driver)?;
                    // SAFETY: Links::try_new only inspects capacity before allocating;
                    // it neither exposes this pool nor submits. Machine installs and
                    // retains the completed pool through shutdown and finish.
                    let pool = unsafe { bound.bind(route) };
                    let links = liveness::Links::try_new(pool, main, io, timer)?;
                    let side = self.aux.start(drive.target());
                    engine::Machine {
                        links,
                        app,
                        controller: ctl,
                        side,
                        shutdown: stop,
                        configuration: engine::Config {
                            connection_lifetime: ttl,
                        },
                        drive,
                    }
                };
                Ok(build(machine, owner))
            },
        )?
    }
}

impl<'source, 'd, T: dope_net::Transport, ID: Identity + 'd, R, A>
    Startup<
        R,
        A,
        queue::Control<'source, 'd, T, ID>,
        side::Disabled,
        &'source queue::Source<'d, T, ID>,
    >
{
    pub fn new(
        app: A,
        source: &'source queue::Source<'d, T, ID>,
        cap: Capacity,
        egress: egress::Config,
        wire: R,
    ) -> Self {
        Self {
            capacities: side::Capacities::standard(cap),
            egress,
            wire,
            connection_lifetime: None,
            app,
            ctl: source.control(),
            aux: side::Disabled,
            source,
        }
    }

    pub fn try_with<'w, E, C, O>(
        self,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'pool> FnOnce(
            engine::Machine<'pool, 'w, 'd, ID, A, queue::Control<'source, 'd, T, ID>, E>,
            &mut C,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        'd: 'w,
        A: app::Receive<'d, ID>,
        (A::Input, A::Continuation): retain::Policy<'w, 'd, ID, A>,
        E: env::Spec<Wire = A::Wire, Transport = T>,
        R: change::Prepare<'w, 'd, ID, A::Wire>,
    {
        let source = self.source;
        let cap = self.capacities.primary();
        self.build((), cap, owner, context, RoutePlan::Attempts(source), build)
    }
}

impl<R, A, S, X> Startup<R, A, S, X> {
    pub(crate) fn try_with<'w, 'd: 'w, ID: Identity + 'd, E, C, O>(
        self,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'pool> FnOnce(engine::Machine<'pool, 'w, 'd, ID, A, S, E, X>, &mut C) -> O,
    ) -> Result<O, creation::Error>
    where
        A: app::Receive<'d, ID>,
        (A::Input, A::Continuation): retain::Policy<'w, 'd, ID, A>,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        R: change::Prepare<'w, 'd, ID, A::Wire>,
        X: startup::Startup<'d, ID>,
    {
        let (aux, io, total) = self
            .aux
            .prepare::<<E::Transport as dope_net::Transport>::Prepared>(
                self.capacities,
                &mut context(owner),
            )?;
        Self { aux, ..self }.build(io, total, owner, context, RoutePlan::Owned, build)
    }
}
