use std::{io, net};

use crate::{
    backend::uring::{descriptor, ops::reservation},
    driver::{self, ops},
    io::{fd::handles, socket},
};

impl<'d> ops::Bootstrap<'d> for driver::Context<'_, 'd> {
    fn bind_listener_slot(
        &mut self,
        addr: net::SocketAddr,
        backlog: i32,
        config: &socket::ListenerConfig,
    ) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)> {
        bind(self, addr, socket::Kind::Stream, config, Some(backlog))
    }

    fn bind_datagram_slot_raw(
        &mut self,
        addr: net::SocketAddr,
    ) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)> {
        use crate::io::socket::ListenerConfig;

        let config = ListenerConfig::for_datagram(&addr);
        bind(self, addr, socket::Kind::Dgram, &config, None)
    }
}

fn bind<'d>(
    driver: &mut driver::Context<'_, 'd>,
    addr: net::SocketAddr,
    kind: socket::Kind,
    config: &socket::ListenerConfig,
    backlog: Option<i32>,
) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)> {
    let domain = socket::Domain::for_addr(&addr);
    if backlog.is_some() {
        let handle = descriptor::Handle::blocking(domain, kind)?;
        return bind_handle(driver, addr, config, backlog, handle);
    }
    let handle = descriptor::Handle::nonblocking(domain, kind)?;
    bind_handle(driver, addr, config, backlog, handle)
}

fn bind_handle<'d>(
    driver: &mut driver::Context<'_, 'd>,
    addr: net::SocketAddr,
    config: &socket::ListenerConfig,
    backlog: Option<i32>,
    handle: descriptor::Handle,
) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)> {
    handle.apply_reuse(config)?;
    if let Some(backlog) = config.fast_open_backlog() {
        handle.setsockopt_raw(libc::IPPROTO_TCP, libc::TCP_FASTOPEN, backlog.get())?;
    }
    handle.bind(&socket::Addr::from_std(addr))?;
    if let Some(backlog) = backlog {
        handle.listen(backlog)?;
    }
    let actual = handle.local_addr()?;
    let descriptor = reservation::Vacant::install_handle(driver, handle)?;
    Ok((descriptor, actual))
}
