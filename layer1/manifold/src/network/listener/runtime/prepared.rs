use std::marker;

use dope_core::driver::{
    self,
    route::{Identity, lease},
    schedule::ready::task::owner,
};
use dope_net::{
    Listen, Transport,
    link::{
        egress::data::{self, compose, exec},
        pool::change,
    },
};

use crate::{
    app::{creation, env, receive},
    network::listener::{
        self, IOV_CAP, config, conn, handler,
        runtime::{self, accept},
        writer::{self, memory},
    },
};

/// Listener storage retained before its client-dependent application is built.
///
/// Only the already admitted payload representation determines these loans.
/// The application and its owned connection slab enter in the consuming build.
pub struct Prepared<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, E, R, B>
where
    E: env::Spec,
    E::Transport: Listen,
    B: data::raw::Payload<'d> + 'd,
{
    pub(super) config: config::Prepared<E::Transport>,
    pub(super) wire: R,
    pub(super) bound: Option<<E::Transport as Listen>::BoundListener>,
    pub(super) lifetime: Option<handler::Lifetime>,
    pub(super) application: Option<handler::Timing>,
    pub(super) accept: accept::Prepared<'d, ID>,
    pub(super) writes: &'p memory::Arena<'s, 'd, ID, B>,
    pub(super) pool: change::raw::Storage<
        'n,
        'd,
        ID,
        compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
        IOV_CAP,
        <B::Mode as exec::Mode>::Reserve<{ memory::WRITE_BUF_CAP }>,
    >,
    pub(super) owner: marker::PhantomData<fn(&'w ()) -> &'w ()>,
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity + 'd, E, R, B> Prepared<'n, 'p, 's, 'w, 'd, ID, E, R, B>
where
    E: env::Spec,
    E::Transport: Listen,
    E::Wire: 'd,
    B: data::raw::Payload<'d> + 'd,
    R: change::Prepare<'w, 'd, ID, E::Wire>,
{
    /// Carries this preparation's admitted capacity into application creation.
    pub fn binding(&self, driver: driver::Reference<'d>) -> driver::Binding<'d> {
        let capacity = owner::Capacity::from(self.config.connections());
        driver::Binding::new(driver.timer(), capacity)
    }

    /// Consumes the storage loans together with their exact application.
    pub fn build<A>(
        self,
        app: A,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>, creation::Error>
    where
        A: handler::Application<'d, ID, Wire = E::Wire, Egress = B> + handler::Runtime<'w, 'd, ID>,
    {
        use lease::Owner;

        let Self {
            config,
            wire,
            bound,
            lifetime,
            application: timing,
            accept,
            writes,
            pool,
            owner: _,
        } = self;
        let conns = config.connections();
        let retain = app.retained(conns).map_err(creation::Error::Config)?;
        let bytes = app.resident(conns).map_err(creation::Error::Config)?;
        let recv = <A::Input as receive::Resources>::provision(bytes, driver)?;
        let pool = pool
            .prepare::<E::Transport, E::Wire, conn::State<'p, 's, 'd, ID, A::Conn, B>, A::Input, R>(
                retain, bytes, wire, driver,
            )?;
        let base = lifetime.unwrap_or_else(handler::Lifetime::persistent);
        let times = timing
            .unwrap_or_default()
            .combine(<A as handler::Application<'d, ID>>::timing(&app));
        let sched = runtime::Schedule::new();
        let mut route = Owner::try_reserve(driver).map_err(creation::Error::Route)?;
        let bound = match bound {
            Some(source) => <E::Transport as Listen>::adopt(route.driver(), source),
            None => <E::Transport as Listen>::bind(
                route.driver(),
                &config.bind,
                config.backlog,
                &config.transport,
            ),
        };
        let (fd, addr) = bound.map_err(creation::Error::Binding)?;
        let opts = <E::Transport as Transport>::options(config.stream);
        let accept = accept
            .bind(fd, conns, opts)
            .map_err(creation::Error::Allocation)?;
        let (pool, pool_req) = pool;
        let (accept, acc_req) = accept.into_parts();
        let flights = route
            .driver()
            .storage
            .reborrow()
            .try_flights(pool_req.pair(acc_req))
            .map_err(creation::Error::Allocation)?;
        let (pool_ops, acc_ops) = flights.slots();
        let pool = pool.commit(pool_ops);
        let accept = accept.commit(acc_ops);
        let route = route.into_claim();
        // SAFETY: pool and claim belong to this installation's route; the exact
        // flight reservation was committed above. Listener owns the resulting
        // pool through shutdown and finish, before any operation can submit.
        let pool = unsafe { pool.bind(route) };
        Ok(listener::Listener {
            owner: writer::Owner::try_new::<E::Timing>(pool, writes, base, times)
                .map_err(creation::Error::Heap)?,
            accept,
            bound_addr: addr,
            app,
            receive_authority: recv,
            schedule: sched,
        })
    }
}
