use std::io;
use std::net::SocketAddr;

use crate::backend::Backend;
use crate::backend::ops::raw::bootstrap::BootstrapBackend;
use crate::io::fd::Fd;
use crate::io::socket::ListenerConfig;

use super::DriverContext;

pub trait Bootstrap<'d> {
    fn bind_listener_slot(
        &mut self,
        addr: SocketAddr,
        backlog: i32,
        config: &ListenerConfig,
    ) -> io::Result<(Fd<'d>, SocketAddr)>;
    fn bind_datagram_slot(&mut self, addr: SocketAddr) -> io::Result<(Fd<'d>, SocketAddr)>;
}

impl<'d> Bootstrap<'d> for DriverContext<'_, 'd> {
    fn bind_listener_slot(
        &mut self,
        addr: SocketAddr,
        backlog: i32,
        config: &ListenerConfig,
    ) -> io::Result<(Fd<'d>, SocketAddr)> {
        <Backend as BootstrapBackend>::bind_listener_slot(self, addr, backlog, config)
    }

    fn bind_datagram_slot(&mut self, addr: SocketAddr) -> io::Result<(Fd<'d>, SocketAddr)> {
        <Backend as BootstrapBackend>::bind_datagram_slot(self, addr)
    }
}
