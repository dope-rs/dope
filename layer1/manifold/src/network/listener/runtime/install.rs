//! Typed, runtime-scoped listener installation.

use std::marker;

use dope_core::driver::{self, route::Identity, schedule::ready::task::owner, storage};
use dope_net::link::{egress::data, pool::change};
use dope_runtime::{random, topology::lending};

use crate::{
    app::{
        creation,
        env::{self, quota},
    },
    network::listener::{self, IOV_CAP, config, handler, runtime::accept, writer::memory},
};

/// Owns every transport choice required to install one listener.
/// Retains its wire storage borrow until the driver installs it.
///
/// ```compile_fail
/// use dope_manifold::network::listener;
/// use dope_net::Listen;
///
/// fn owner_lifetime_cannot_be_widened<'w, E>(
///     install: listener::Install<'w, dope_core::driver::route::KeyTag<0>, E, dope_net::wire::domain::Endpoint<'w, E::Wire, dope_core::driver::route::KeyTag<0>>>,
/// ) -> listener::Install<'static, dope_core::driver::route::KeyTag<0>, E, dope_net::wire::domain::Endpoint<'w, E::Wire, dope_core::driver::route::KeyTag<0>>>
/// where
///     E: dope_manifold::app::env::Spec,
///     E::Transport: Listen,
/// {
///     install
/// }
/// ```
pub struct Install<'w, ID: Identity, E, R>
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
{
    config: config::Prepared<E::Transport>,
    wire: R,
    bound: Option<<E::Transport as dope_net::Listen>::BoundListener>,
    lifetime: Option<handler::Lifetime>,
    application: Option<handler::Timing>,
    env: crate::Family<(E, ID)>,
    /// Retains the exact wire-owner lifetime independently of driver generation.
    owner: marker::PhantomData<fn(&'w ()) -> &'w ()>,
}

impl<'w, ID: Identity, E, R> Install<'w, ID, E, R>
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
{
    pub fn new(config: config::Prepared<E::Transport>, wire: R) -> Self {
        Self {
            config,
            wire,
            bound: None,
            lifetime: None,
            application: None,
            env: marker::PhantomData,
            owner: marker::PhantomData,
        }
    }

    /// Binds application construction to this listener's capacity and driver clock.
    pub fn binding<'d: 'w>(&self, driver: driver::Reference<'d>) -> driver::Binding<'d> {
        let capacity = owner::Capacity::from(self.config.connections());
        driver::Binding::new(driver.timer(), capacity)
    }

    pub fn with_bound(
        mut self,
        bound: Option<<E::Transport as dope_net::Listen>::BoundListener>,
    ) -> Self {
        self.bound = bound;
        self
    }

    /// Replaces the env default with one fully validated listener
    /// lifetime policy. The value is consumed before admission is enabled.
    pub fn with_life(mut self, life: handler::Lifetime) -> Self {
        self.lifetime = Some(life);
        self
    }

    /// Installs an independent application deadline root without granting
    /// authority over connection retirement.
    pub fn with_timing(mut self, timing: handler::Timing) -> Self {
        self.application = Some(timing);
        self
    }

    /// Installs using the canonical listener hash domain for `ID`.
    pub fn in_stage<'e, 'd, 'stage, 'scope, 'life, A, S, Q, O>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        app: A,
        build: impl for<'n, 'p, 's> FnOnce(
            listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        S: storage::Plan,
        ID: 'd,
        'd: 'life + 'w,
        'life: 'scope,
        A: handler::Application<'d, ID, Wire = E::Wire> + handler::Runtime<'w, 'd, ID>,
        E::Wire: 'd,
        R: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        let hash = stage.hash_state(listener::Domain::for_listener(ID::ID.raw()));
        self.with_driver(app, hash, stage, lending::Stage::driver, build)
    }

    /// Installs a cold application within the backing storage loan.
    pub fn build_in<'e, 'd, 'stage, 'scope, 'life, A, S, Q, FactoryError, O>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        make: impl FnOnce(driver::Binding<'d>) -> Result<A, FactoryError>,
        build: impl for<'n, 'p, 's> FnOnce(
            listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        ) -> O,
    ) -> Result<O, crate::BuildError<FactoryError>>
    where
        S: storage::Plan,
        ID: 'd,
        'd: 'life + 'w,
        'life: 'scope,
        A: handler::Application<'d, ID, Wire = E::Wire> + handler::Runtime<'w, 'd, ID>,
        E::Wire: 'd,
        R: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        let hash = stage.hash_state(listener::Domain::for_listener(ID::ID.raw()));
        self.build(hash, stage, lending::Stage::driver, make, build)
    }

    /// Reserves this listener's route and storage in the matching driver domain.
    pub fn with_driver<'d: 'w, A, C, O>(
        self,
        app: A,
        hash: random::HashState<'d>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'n, 'p, 's> FnOnce(
            listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>,
            &mut C,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        ID: 'd,
        A: handler::Application<'d, ID, Wire = E::Wire> + handler::Runtime<'w, 'd, ID>,
        E::Wire: 'd,
        R: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        self.prepare(hash, owner, &context, |prepared, owner| {
            let listener = prepared.build(app, &mut context(owner))?;
            Ok(build(listener, owner))
        })?
    }

    /// Opens every backing-storage loan before constructing the application.
    pub fn prepare<'d: 'w, B, C, O>(
        self,
        hash: random::HashState<'d>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'n, 'p, 's> FnOnce(
            listener::Prepared<'n, 'p, 's, 'w, 'd, ID, E, R, B>,
            &mut C,
        ) -> O,
    ) -> Result<O, creation::Error>
    where
        ID: 'd,
        B: data::raw::Payload<'d> + 'd,
        E::Wire: 'd,
        R: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        let Self {
            config: plan,
            wire: end,
            bound,
            lifetime: life,
            application: times,
            env: _,
            owner: _,
        } = self;
        use memory::geometry::Geometry;
        let conns = plan.connections();
        let count = conns.entries().get();
        let shape = Geometry::<B::Mode>::resolve(plan.egress, plan.direct_flights, conns)
            .map_err(creation::Error::Config)?;
        let maximum = context(owner).driver_ref().accept_cap().count().get();
        if count > maximum {
            return Err(creation::Error::Config(
                crate::ConfigError::ListenerAcceptCapacity {
                    actual: count,
                    maximum,
                },
            ));
        }
        let limit = plan
            .per_ip
            .unwrap_or(<E::Admission as quota::Policy>::PER_IP);
        let accept = accept::Prepared::<ID>::try_new(conns.into(), limit, hash)
            .map_err(accept::Prepare::into_error)?;
        let driver = context(owner).driver_ref();
        memory::Arena::<ID, B>::try_with(shape, conns.into(), driver, |writes, setup| {
            change::raw::Storage::<ID, _, IOV_CAP, _>::try_with(
                setup,
                owner,
                &context,
                |pool, owner| {
                    build(
                        listener::Prepared {
                            config: plan,
                            wire: end,
                            bound,
                            lifetime: life,
                            application: times,
                            accept,
                            writes,
                            pool,
                            owner: marker::PhantomData,
                        },
                        owner,
                    )
                },
            )
            .map_err(creation::Error::from)
        })
        .map_err(creation::Error::Allocation)?
    }

    pub(crate) fn build<'d: 'w, A, FactoryError, C, O>(
        self,
        hash: random::HashState<'d>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        make: impl FnOnce(driver::Binding<'d>) -> Result<A, FactoryError>,
        build: impl for<'n, 'p, 's> FnOnce(
            listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>,
            &mut C,
        ) -> O,
    ) -> Result<O, crate::BuildError<FactoryError>>
    where
        ID: 'd,
        A: handler::Application<'d, ID, Wire = E::Wire> + handler::Runtime<'w, 'd, ID>,
        E::Wire: 'd,
        R: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        let binding = self.binding(context(owner).driver_ref());
        let app = make(binding).map_err(crate::BuildError::Factory)?;
        self.with_driver(app, hash, owner, context, build)
            .map_err(crate::BuildError::Create)
    }
}
