use std::{
    cell::Cell,
    net::{SocketAddr, TcpListener},
};

use dope_core::{
    driver::{self, route::KeyTag},
    io::{
        fd::handles,
        socket::{self, install::Install, option::Set},
    },
};
use dope_manifold::{
    app::env::Bundle,
    network::listener::{self, handler},
    timing::Throughput,
};
use dope_net::{self as net, tcp::Tcp, wire::Identity};
use dope_runtime::control;
use dope_test::harness::signals::LocalGateRef;

pub struct Native;

impl net::Transport for Native {
    type Addr = SocketAddr;
    type Prepared = socket::Inet;
    type StreamConfig = <Tcp as net::Transport>::StreamConfig;

    fn options(config: Self::StreamConfig) -> Set {
        <Tcp as net::Transport>::options(config)
    }
}

impl net::Listen for Native {
    type ListenerConfig = <Tcp as net::Listen>::ListenerConfig;
    type BoundListener = TcpListener;

    fn bind<'d>(
        driver: &mut driver::Context<'_, 'd>,
        addr: &SocketAddr,
        cap: socket::Backlog,
        config: &Self::ListenerConfig,
    ) -> Result<(handles::Descriptor<'d>, SocketAddr), net::BindingError> {
        <Tcp as net::Listen>::bind(driver, addr, cap, config)
    }

    fn adopt<'d>(
        driver: &mut driver::Context<'_, 'd>,
        bound: TcpListener,
    ) -> Result<(handles::Descriptor<'d>, SocketAddr), net::BindingError> {
        Install::adopt_stream(driver.reborrow(), bound.into()).map_err(net::BindingError::Bootstrap)
    }
}

pub type Env = Bundle<Native, Identity, Throughput>;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime, driver = 'd)]
#[coordinate]
pub struct Host<'a, 'n, 'p, 's, 'd: 's, A>
where
    A: handler::Application<'d, KeyTag<0>, Wire = Identity> + handler::Runtime<'d, 'd, KeyTag<0>>,
{
    #[pin]
    #[manifold(control)]
    pub listener: listener::Listener<'n, 'p, 's, 'd, 'd, KeyTag<0>, A, Env>,
    #[dispatcher(state)]
    pub observed: &'a Cell<Option<listener::AcceptError>>,
    #[dispatcher(state)]
    pub ready: LocalGateRef<'a>,
}

impl<'a, 'n, 'p, 's, 'd, A> Host<'a, 'n, 'p, 's, 'd, A>
where
    A: handler::Application<'d, KeyTag<0>, Wire = Identity> + handler::Runtime<'d, 'd, KeyTag<0>>,
{
    fn coordinate(this: host_coord::Fields<'_, 'a, 'n, 'p, 's, 'd, A>) -> control::Flow {
        if this.observed.get().is_none()
            && let Some(cause) = this.listener.accept_error()
        {
            this.observed.set(Some(cause));
            this.ready.hit();
        }
        control::Flow::Idle
    }
}
