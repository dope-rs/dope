use std::{convert::Infallible, marker, mem};

use dope_core::driver::{
    route::Identity,
    settings,
    settings::{claim, errors},
};
use dope_manifold::{
    app::{env, receive},
    client::{app::request, session},
    network::{
        client::topology::{
            factory::{Application, disabled},
            plan,
        },
        listener::{handler, topology::factory},
    },
};
use dope_net::{
    tcp,
    wire::{self, domain},
};
use session::{
    app::request::Source,
    codec::{self, staging},
    ingress,
    policy::Compatible,
};

/// One test application and receive contract consumed at installation.
pub struct Value<T, I = ()> {
    value: T,
    input: marker::PhantomData<I>,
}

impl<T, I> Value<T, I> {
    pub const fn new(value: T) -> Self {
        Self {
            value,
            input: marker::PhantomData,
        }
    }
}

impl<T, I> settings::Resource for Value<T, I> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<T, I: receive::Resources> factory::ReceiveSpec for Value<T, I> {
    type Input = I;
}

impl<ID: Identity, T, B, I, E> factory::Direct<ID, E> for Value<(T, B), I>
where
    E: env::Spec<Transport = tcp::Tcp>,
    <E::Wire as wire::Spec>::Bootstrap: domain::Init<E::Wire>,
    for<'d> <E::Wire as wire::Spec>::Binding<'d, ID>: From<B>,
    I: receive::Mode<E::Wire> + receive::Resources,
    for<'d> T:
        handler::Application<'d, ID, Wire = E::Wire, Input = I> + handler::Runtime<'d, 'd, ID>,
{
    type Application<'d>
        = T
    where
        ID: 'd,
        Self: 'd,
        E::Wire: 'd;
    type Error = Infallible;

    type Wire<'d>
        = domain::Local<'d, E::Wire, ID>
    where
        ID: 'd,
        Self: 'd,
        E::Wire: 'd;

    fn build<'d>(self) -> Result<(Self::Application<'d>, Self::Wire<'d>), Self::Error>
    where
        ID: 'd,
        Self: 'd,
        E::Wire: 'd,
        Self::Input: 'd,
    {
        let (app, binding) = self.value;
        Ok((app, domain::Local::new(binding.into())))
    }
}

const _: () = {
    assert!(size_of::<Value<usize, [u8; 4096]>>() == size_of::<usize>());
    assert!(align_of::<Value<usize, [u8; 4096]>>() == align_of::<usize>());
    assert!(mem::needs_drop::<Value<String>>() == mem::needs_drop::<String>());
};

impl<ID: Identity, const MAX: usize, N, E> Application<(), ID, MAX, E> for Value<N>
where
    E: env::Spec,
    <E::Wire as wire::Spec>::Bootstrap: domain::Init<E::Wire>,
    for<'d> N: session::Target<'d, ID, MAX>
        + session::Connected<'d, ID>
        + session::Exhausted<'d, ID>
        + session::Failed<'d, ID>
        + Source<'d, ID>
        + Compatible<'d, 'd, E::Wire, ID>,
    for<'d> <N as session::Session<'d, ID>>::Codec: codec::Codec<Staging = staging::Buffered>,
    for<'d> <E::Wire as wire::Spec>::Binding<'d, ID>: Default,
{
    type Input = plan::Local;
    type Session<'life, 'p, 'd>
        = N
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        E::Wire: 'd;
    type Source<'life, 'p, 'd>
        = ingress::None
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        E::Wire: 'd;
    type Wire<'life, 'p, 'd>
        = domain::Local<'d, E::Wire, ID>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        E::Wire: 'd;
    type Error = Infallible;

    fn build<'life, 'p, 'd>(
        self,
        _: &'life (),
    ) -> Result<
        (
            session::Setup<'d, Self::Session<'life, 'p, 'd>, Self::Source<'life, 'p, 'd>>,
            Self::Wire<'life, 'p, 'd>,
        ),
        Self::Error,
    >
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        E::Wire: 'd,
    {
        Ok((
            session::Setup::new(self.value, ingress::Plan::none()),
            domain::Local::new(Default::default()),
        ))
    }
}

impl<ID: Identity, const MAX: usize, N, E> disabled::Application<(), ID, MAX, E> for Value<N>
where
    E: env::Spec,
    <E::Wire as wire::Spec>::Bootstrap: domain::Init<E::Wire>,
    for<'d> N: session::Target<'d, ID, MAX>
        + session::Connected<'d, ID>
        + session::Exhausted<'d, ID>
        + session::Failed<'d, ID>
        + session::Session<'d, ID, Requests = request::Disabled>
        + Compatible<'d, 'd, E::Wire, ID>,
    for<'d> <N as session::Session<'d, ID>>::Codec: codec::Codec<Staging = staging::Buffered>,
    for<'d> <E::Wire as wire::Spec>::Binding<'d, ID>: Default,
{
    type Input = plan::Local;
    type Session<'life, 'p, 'd>
        = N
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        E::Wire: 'd;
    type Source<'life, 'p, 'd>
        = ingress::None
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        E::Wire: 'd;
    type Wire<'life, 'p, 'd>
        = domain::Local<'d, E::Wire, ID>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        E::Wire: 'd;
    type Error = Infallible;

    fn build<'life, 'p, 'd>(
        self,
        _: &'life (),
    ) -> Result<
        (
            session::Setup<'d, Self::Session<'life, 'p, 'd>, Self::Source<'life, 'p, 'd>>,
            Self::Wire<'life, 'p, 'd>,
        ),
        Self::Error,
    >
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        ID: 'd,
        E::Wire: 'd,
    {
        Ok((
            session::Setup::new(self.value, ingress::Plan::none()),
            domain::Local::new(Default::default()),
        ))
    }
}
