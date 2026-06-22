use std::{io, net::SocketAddr};

use super::sqe::Sqe;
use super::{BOOT_UD, Driver};
use crate::socket::{Domain, Fd, FdSlot, Kind};
use crate::{Bootstrap, ListenerOpts};

impl Bootstrap for Driver {
    fn bind_listener_slot(
        &mut self,
        addr: SocketAddr,
        backlog: i32,
        opts: &ListenerOpts,
    ) -> io::Result<(Fd, SocketAddr)> {
        let (idx, bound) = if addr.port() == 0 {
            self.boot_bound_via_syscall(addr, Kind::Stream, opts, Some(backlog))?
        } else {
            let idx = self.boot_bind_slot(Domain::for_addr(&addr), Kind::Stream, addr, Some(opts))?;
            self.boot_perform(Sqe::listen_at(FdSlot::new(idx), backlog, BOOT_UD))?;
            (idx, addr)
        };
        Ok((Fd::adopt(FdSlot::new(idx), self), bound))
    }

    fn bind_datagram_slot(&mut self, addr: SocketAddr) -> io::Result<(Fd, SocketAddr)> {
        let (idx, bound) = if addr.port() == 0 {
            let no_opts = ListenerOpts { reuse_addr: false, reuse_port: false, fastopen_backlog: None, defer_accept_secs: None };
            self.boot_bound_via_syscall(addr, Kind::Dgram, &no_opts, None)?
        } else {
            let idx = self.boot_bind_slot(Domain::for_addr(&addr), Kind::Dgram, addr, None)?;
            (idx, addr)
        };
        Ok((Fd::adopt(FdSlot::new(idx), self), bound))
    }
}