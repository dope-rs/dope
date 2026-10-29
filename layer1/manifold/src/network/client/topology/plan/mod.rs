use std::{marker, mem};

use dope_core::driver::{
    self,
    route::{Identity, KeyTag},
    settings::{self, Domain, DomainSum, claim, errors},
};
use dope_net::link::pool::change;
use dope_runtime::random;
use o3::base::time::Positive;
use session::{
    codec::staging,
    ingress::{Source, origin},
};

use crate::{
    app::{creation, env},
    client::{session, side, side::registry},
    network::client::{self, topology},
    service,
    service::{discover, observe, sync},
};

mod layout;
mod spec;
mod staged;

pub use layout::Layout;
mod seal;

pub(crate) use seal::Seal;
pub use spec::Spec;
/// Prepared connection state for the selected ingress staging policy.
pub use staged::Staged;

/// Driver-owned ingress authority with statically selected staging.
///
/// Local authority cannot be substituted for externally supplied authority.
///
/// ```compile_fail
/// use dope_manifold::client::session::ingress::origin;
/// use dope_manifold::network::client::topology::plan::{Input, Local};
///
/// fn require_external<A: Input<Authority = origin::External>>() {}
///
/// require_external::<Local>();
/// ```
///
/// The default buffered staging cannot be substituted for direct staging.
///
/// ```compile_fail
/// use dope_manifold::client::session::codec::staging;
/// use dope_manifold::network::client::topology::plan::{Input, Local};
///
/// fn require_direct<A: Input<Staging = staging::Direct>>() {}
///
/// require_direct::<Local>();
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Local<P = staging::Buffered>(marker::PhantomData<fn() -> P>);

/// Externally supplied ingress authority with statically selected staging.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct External<P = staging::Buffered>(marker::PhantomData<fn() -> P>);

impl<P> Seal for Local<P> {}

impl<P> Seal for External<P> {}

pub trait Input: Seal {
    const DOMAINS: Result<Domain, DomainSum>;
    type Authority: origin::Policy;
    type Staging: staging::Policy;
}

impl<P: staging::Policy> Input for Local<P> {
    const DOMAINS: Result<Domain, DomainSum> =
        <origin::Local as origin::Policy>::DOMAINS.combine(<P as staging::Seal>::DOMAINS);
    type Authority = origin::Local;
    type Staging = P;
}

impl<P: staging::Policy> Input for External<P> {
    const DOMAINS: Result<Domain, DomainSum> =
        <origin::External as origin::Policy>::DOMAINS.combine(<P as staging::Seal>::DOMAINS);
    type Authority = origin::External;
    type Staging = P;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Standard;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Side;

#[derive(PartialEq, Eq)]
pub struct Plan<ID: Identity, const MAX: usize, M = Standard, A = Local, B = Spec> {
    layout: Layout<ID, MAX, M, A>,
    state: B,
}

impl<ID: Identity, const MAX: usize, M, A> Plan<ID, MAX, M, A> {
    #[must_use]
    pub const fn with_ttl(mut self, ttl: Positive) -> Self {
        self.state = self.state.with_ttl(ttl);
        self
    }

    /// Binds the wire owner and domain hash before installing the client.
    pub fn bind<'d, W>(
        self,
        wire: W,
        hash: random::HashState<'d>,
    ) -> Plan<ID, MAX, M, A, client::Config<'d, W>>
    where
        A: Input,
    {
        let config = self.state.bind(self.layout.capacities(), hash, wire);
        Plan {
            layout: self.layout,
            state: config,
        }
    }
}

impl<ID: Identity, const MAX: usize, A: Input> Plan<ID, MAX, Standard, A> {
    pub fn new(count: usize, spec: Spec) -> Result<Self, errors::Claim> {
        Layout::<ID, MAX, Standard, A>::new(count).map(|layout| layout.into_plan(spec))
    }
}

impl<'d, ID: Identity + 'd, const MAX: usize, A: Input, W>
    Plan<ID, MAX, Standard, A, client::Config<'d, W>>
{
    pub fn install<'w, N, S, R, I, E, const ENDPOINTS: usize, C, U>(
        self,
        setup: session::Setup<'d, N, S>,
        set: service::Snapshot<
            service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
            ENDPOINTS,
        >,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'q, 'h, 'p> FnOnce(
            client::Pool<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS>,
            &mut C,
        ) -> U,
    ) -> Result<U, creation::Error>
    where
        'd: 'w,
        N: session::Target<'d, ID, MAX> + Staged<'d, ID, A::Staging>,
        S: Source<'d, Authority = A::Authority>,
        E: env::Spec,
        E::Wire: 'd,
        E::Transport: dope_net::Transport,
        I: Eq,
        R: sync::Policy,
        W: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        client::Pool::try_with(
            setup,
            set,
            self.state,
            side::Disabled,
            owner,
            context,
            build,
        )
    }

    pub fn watch<'w, N, S, D, R, I, E, const ENDPOINTS: usize, C, U>(
        self,
        setup: session::Setup<'d, N, S>,
        set: service::Snapshot<
            service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
            ENDPOINTS,
        >,
        demand: D,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'q, 'h, 'p> FnOnce(
            client::Connector<
                'q,
                'h,
                'p,
                'w,
                'd,
                ID,
                MAX,
                N,
                discover::Watched<D>,
                R,
                I,
                (),
                E,
                ENDPOINTS,
            >,
            &mut C,
        ) -> U,
    ) -> Result<U, creation::Error>
    where
        'd: 'w,
        N: session::Target<'d, ID, MAX> + Staged<'d, ID, A::Staging>,
        S: Source<'d, Authority = A::Authority>,
        E: env::Spec,
        E::Wire: 'd,
        E::Transport: dope_net::Transport,
        I: Eq,
        R: sync::Policy,
        D: discover::Demand<'d, Endpoint = I>,
        W: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        client::Connector::try_watch(setup, demand, set, self.state, owner, context, build)
    }

    pub fn discover<'w, N, S, D, R, I, O, E, const ENDPOINTS: usize, C, U>(
        self,
        setup: session::Setup<'d, N, S>,
        lookup: D,
        watch: O,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'q, 'h, 'p> FnOnce(
            client::Connector<
                'q,
                'h,
                'p,
                'w,
                'd,
                ID,
                MAX,
                N,
                discover::Refreshing<'d, D>,
                R,
                I,
                O,
                E,
                ENDPOINTS,
            >,
            &mut C,
        ) -> U,
    ) -> Result<U, creation::Error>
    where
        'd: 'w,
        N: session::Target<'d, ID, MAX> + Staged<'d, ID, A::Staging>,
        S: Source<'d, Authority = A::Authority>,
        E: env::Spec,
        E::Wire: 'd,
        E::Transport: dope_net::Transport,
        I: Eq,
        R: sync::Policy,
        W: change::Prepare<'w, 'd, ID, E::Wire>,
        D: discover::Output
            + discover::RefreshAt<'d>
            + discover::PollAt<
                'd,
                I,
                <<E as env::Spec>::Transport as dope_net::Transport>::Addr,
                ENDPOINTS,
            >,
        O: observe::Audit<
                'd,
                I,
                <<E as env::Spec>::Transport as dope_net::Transport>::Addr,
                D,
                ENDPOINTS,
            > + observe::Failure<
                'd,
                I,
                <<E as env::Spec>::Transport as dope_net::Transport>::Addr,
                D,
                ENDPOINTS,
            >,
    {
        let at = context(owner).driver_ref().clock().now();
        self.prepare(setup, owner, context, |prepared, owner| {
            build(prepared.discover(lookup, watch, at), owner)
        })
    }

    /// Prepares all borrowed storage before issuing a discovery client.
    ///
    /// The callback owns a cold engine. Its consuming `discover` transition
    /// attaches the client-dependent lookup without reopening the storage scope.
    pub fn prepare<'w, N, S, R, I, E, const ENDPOINTS: usize, C, U>(
        self,
        setup: session::Setup<'d, N, S>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'q, 'h, 'p> FnOnce(
            client::Prepared<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS>,
            &mut C,
        ) -> U,
    ) -> Result<U, creation::Error>
    where
        'd: 'w,
        N: session::Target<'d, ID, MAX> + Staged<'d, ID, A::Staging>,
        S: Source<'d, Authority = A::Authority>,
        E: env::Spec,
        E::Wire: 'd,
        E::Transport: dope_net::Transport,
        I: Eq,
        R: sync::Policy,
        W: change::Prepare<'w, 'd, ID, E::Wire>,
    {
        self.state
            .build::<MAX, N, R, I, E, ENDPOINTS, discover::Notify, side::Disabled, S, ID, C, U>(
                setup,
                client::Seed {
                    feedback: discover::Notify::new(),
                    snapshot: None,
                },
                side::Disabled,
                owner,
                context,
                |engine, owner| build(client::Prepared { engine }, owner),
            )
    }
}

impl<ID: Identity, const MAX: usize, A: Input> Plan<ID, MAX, Side, A> {
    pub fn try_new(count: usize, spec: Spec) -> Result<Self, errors::Claim> {
        Layout::<ID, MAX, Side, A>::try_new(count).map(|layout| layout.into_plan(spec))
    }
}

impl<'d, ID: Identity, const MAX: usize, A: Input, W>
    Plan<ID, MAX, Side, A, client::Config<'d, W>>
{
    /// Runs the consumer while the connection pool's storage remains borrowed.
    pub fn install<'r, N, S, K, R, I, E, const ENDPOINTS: usize, C, U>(
        self,
        setup: session::Setup<'d, N, S>,
        mode: topology::Registered<'r, 'd, ID, K>,
        set: service::Snapshot<
            service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
            ENDPOINTS,
        >,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'q, 'h, 'p> FnOnce(
            client::Pool<
                'q,
                'h,
                'p,
                'd,
                'd,
                ID,
                MAX,
                N,
                R,
                I,
                E,
                ENDPOINTS,
                topology::Registered<'r, 'd, ID, K>,
            >,
            &mut C,
        ) -> U,
    ) -> Result<U, creation::Error>
    where
        N: session::Target<'d, ID, MAX> + Staged<'d, ID, A::Staging>,
        S: Source<'d, Authority = A::Authority>,
        K: registry::Intent<'d, ID>,
        ID: 'd,
        E: env::Spec,
        E::Wire: 'd,
        E::Transport: dope_net::Transport,
        I: Eq,
        R: sync::Policy,
        W: change::Prepare<'d, 'd, ID, E::Wire>,
    {
        client::Pool::try_with(setup, set, self.state, mode, owner, context, build)
    }
}

impl<ID: Identity, const MAX: usize, M, A, B> settings::Resource for Plan<ID, MAX, M, A, B>
where
    Layout<ID, MAX, M, A>: settings::Resource,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.layout.include_in(needs)
    }
}

const _: () = {
    assert!(mem::size_of::<Local>() == 0);
    assert!(mem::size_of::<External<staging::Direct>>() == 0);
    assert!(mem::size_of::<Layout<KeyTag<0>, 1>>() == 2 * mem::size_of::<u32>());
    assert!(
        mem::size_of::<Layout<KeyTag<0>, 1, Side, External<staging::Direct>>>()
            == 2 * mem::size_of::<u32>()
    );
};
