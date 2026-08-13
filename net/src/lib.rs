#![doc = include_str!("compile_fail.md")]

pub mod link;
pub mod tcp;
pub mod unix;
pub mod wire;

use std::{io, net};

use dope_core::{
    driver,
    io::{
        fd::handles,
        socket::{self, option},
    },
};
pub trait Transport: 'static + Sized {
    type Addr;
    type StreamConfig: Default + Copy + 'static;

    fn to_sock_addr(addr: &Self::Addr) -> io::Result<socket::Addr>;

    /// Resolves stream tuning into an owned, allocation-free kernel plan.
    fn stream_options(config: Self::StreamConfig) -> io::Result<option::StreamOptions>;
}

pub trait ListenerTransport: Transport {
    type ListenerConfig: Default + Clone + 'static;

    fn bind_listener_slot<'d>(
        driver: &mut driver::Context<'_, 'd>,
        addr: &Self::Addr,
        backlog: i32,
        config: &Self::ListenerConfig,
    ) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)>;

    fn per_ip_limit(config: &Self::ListenerConfig) -> Option<u32>;
}
