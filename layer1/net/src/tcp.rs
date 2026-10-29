use std::net;

pub use dope_core::io::socket::Config;
use dope_core::{
    driver,
    io::{
        fd::handles,
        socket::{self, install::Install, option},
    },
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamConfig {
    pub recv_buffer_size: Option<option::Buffer>,
    pub send_buffer_size: Option<option::Buffer>,
    pub no_delay: Option<option::Switch>,
    pub keep_alive: Option<option::Switch>,
    pub keep_alive_idle: Option<option::Period>,
    pub keep_alive_interval: Option<option::Period>,
    pub keep_alive_retries: Option<option::Retries>,
}

pub struct Tcp;

impl crate::Transport for Tcp {
    type Addr = net::SocketAddr;
    type Prepared = socket::Inet;
    type StreamConfig = StreamConfig;

    fn options(config: StreamConfig) -> option::Set {
        option::Set::new(
            config.recv_buffer_size,
            config.send_buffer_size,
            config.no_delay,
            config.keep_alive,
            config.keep_alive_idle,
            config.keep_alive_interval,
            config.keep_alive_retries,
        )
    }
}

impl crate::Listen for Tcp {
    type ListenerConfig = Config;
    type BoundListener = socket::Listener;

    fn bind<'d>(
        driver: &mut driver::Context<'_, 'd>,
        addr: &net::SocketAddr,
        cap: socket::Backlog,
        config: &Config,
    ) -> Result<(handles::Descriptor<'d>, net::SocketAddr), crate::BindingError> {
        Install::bind_stream(driver.reborrow(), *addr, cap, config)
            .map_err(crate::BindingError::Bootstrap)
    }

    fn adopt<'d>(
        driver: &mut driver::Context<'_, 'd>,
        bound: socket::Listener,
    ) -> Result<(handles::Descriptor<'d>, net::SocketAddr), crate::BindingError> {
        Install::adopt_bound(driver.reborrow(), bound).map_err(crate::BindingError::Bootstrap)
    }
}
