use std::{convert::Infallible, marker, mem};

use dope_core::driver::{
    route::Identity,
    settings,
    settings::{claim, errors},
};
use dope_manifold::{
    app::{env, receive},
    client::session,
    network::{
        client::topology::{factory::Application, plan},
        listener::{handler, topology::factory},
    },
};
use dope_net::{
    link::pool::change,
    wire::{self, domain},
};
use session::{
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
    E: env::Spec,
    E::Transport: dope_net::Listen,
    for<'d> domain::Local<B>: change::Prepare<'d, 'd, ID, E::Wire>,
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
        = domain::Local<B>
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
        Ok((app, domain::Local::new(binding)))
    }
}

const _: () = {
    assert!(size_of::<Value<usize, [u8; 4096]>>() == size_of::<usize>());
    assert!(align_of::<Value<usize, [u8; 4096]>>() == align_of::<usize>());
    assert!(mem::needs_drop::<Value<String>>() == mem::needs_drop::<String>());
};

impl<'life, 'p, 'd, ID: Identity + 'd, const MAX: usize, N, E>
    Application<'life, 'p, 'd, (), ID, MAX, E> for Value<N>
where
    E: env::Spec,
    E::Wire: 'd,
    N: session::Target<'d, ID, MAX>
        + session::Connected<'d, ID>
        + session::Exhausted<'d, ID>
        + session::Failed<'d, ID>
        + Compatible<'d, 'd, E::Wire, ID>,
    <N as session::Session<'d, ID>>::Codec: codec::Codec<Staging = staging::Buffered>,
    <E::Wire as wire::Spec>::Runtime<'d, 'd, ID>: Default,
{
    type Input = plan::Local;
    type Session = N;
    type Source = ingress::None;
    type Wire = domain::Local<<E::Wire as wire::Spec>::Runtime<'d, 'd, ID>>;
    type Error = Infallible;

    fn build(
        self,
        _: &'life (),
    ) -> Result<(session::Setup<'d, Self::Session, Self::Source>, Self::Wire), Self::Error>
    where
        Self: 'd,
        'd: 'p,
        'p: 'life,
    {
        Ok((
            session::Setup::new(self.value, ingress::Plan::none()),
            domain::Local::new(Default::default()),
        ))
    }
}
