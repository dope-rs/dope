use std::net::{SocketAddr, TcpListener};

use dope_core::{
    driver,
    io::{
        fd::handles,
        socket::{self, install::Install, option::Set},
    },
};
use dope_manifold::{app::env::Bundle, timing::Throughput};
use dope_net::{self as net, tcp::Tcp, wire::Identity};

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
