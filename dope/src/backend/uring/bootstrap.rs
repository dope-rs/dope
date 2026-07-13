use std::{io, net::SocketAddr};

use super::sqe::Sqe;
use super::{BOOT_UD, Driver};
use crate::socket::{Domain, Fd, FdSlot, Kind};
use crate::{Bootstrap, ListenerOpts};

impl Bootstrap for Driver {
    fn bind_listener_slot(
        &self,
        addr: SocketAddr,
        backlog: i32,
        opts: &ListenerOpts,
    ) -> io::Result<(Fd<'_>, SocketAddr)> {
        // SAFETY: leaf.
        let this = unsafe { self.inner() };
        let (idx, bound) = if addr.port() == 0 {
            this.boot_bound_via_syscall(addr, Kind::Stream, opts, Some(backlog))?
        } else {
            let idx = this.boot_bind_slot(Domain::for_addr(&addr), Kind::Stream, addr, Some(opts))?;
            this.boot_perform(Sqe::listen_at(FdSlot::new(idx), backlog, BOOT_UD))?;
            (idx, addr)
        };
        Ok((Fd::adopt(FdSlot::new(idx), self), bound))
    }

    fn bind_datagram_slot(&self, addr: SocketAddr) -> io::Result<(Fd<'_>, SocketAddr)> {
        // SAFETY: leaf.
        let this = unsafe { self.inner() };
        let opts = crate::backend::datagram_opts(&addr);
        let (idx, bound) = if addr.port() == 0 {
            this.boot_bound_via_syscall(addr, Kind::Dgram, &opts, None)?
        } else {
            let idx = this.boot_bind_slot(Domain::for_addr(&addr), Kind::Dgram, addr, Some(&opts))?;
            (idx, addr)
        };
        Ok((Fd::adopt(FdSlot::new(idx), self), bound))
    }
}