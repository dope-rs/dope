use std::io;
use std::net::SocketAddr;

use crate::backend::Backend;
use crate::driver::DriverContext;
use crate::io::fd::Fd;
use crate::io::socket::ListenerConfig;

pub(crate) trait BootstrapBackend {
    fn bind_listener_slot<'d>(
        driver: &mut DriverContext<'_, 'd>,
        addr: SocketAddr,
        backlog: i32,
        config: &ListenerConfig,
    ) -> io::Result<(Fd<'d>, SocketAddr)>;
    fn bind_datagram_slot<'d>(
        driver: &mut DriverContext<'_, 'd>,
        addr: SocketAddr,
    ) -> io::Result<(Fd<'d>, SocketAddr)>;
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io::Error;
    use std::os::fd::{AsRawFd, RawFd};

    use io_uring::opcode::FilesUpdate;
    use libc::{
        EMFILE, IPPROTO_TCP, SO_REUSEADDR, SO_REUSEPORT, SOL_SOCKET, TCP_DEFER_ACCEPT, TCP_FASTOPEN,
    };

    use crate::backend::ops::raw::control::ControlBackend;
    use crate::backend::{RawSqe, Sqe};
    use crate::driver::submission::Submission;
    use crate::driver::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token};
    use crate::io::fd::FdSlot;
    use crate::io::ffi::Handle;
    use crate::io::socket::addr::Addr;
    use crate::io::socket::{Domain, Kind};

    use super::{Backend, BootstrapBackend, DriverContext, Fd, ListenerConfig, SocketAddr, io};

    const BOOTSTRAP_UD: Token = Token::new(ROUTE_FRAMEWORK, SlotIndex::new(0), Epoch::ZERO);

    impl BootstrapBackend for Backend {
        fn bind_listener_slot<'d>(
            driver: &mut DriverContext<'_, 'd>,
            addr: SocketAddr,
            backlog: i32,
            config: &ListenerConfig,
        ) -> io::Result<(Fd<'d>, SocketAddr)> {
            let reference = driver.driver_ref();
            let (idx, bound) = if addr.port() == 0 {
                bootstrap_bound_via_syscall(driver, addr, Kind::Stream, config, Some(backlog))
            } else {
                let idx = bootstrap_bind_slot(
                    driver,
                    Domain::for_addr(&addr),
                    Kind::Stream,
                    addr,
                    Some(config),
                )?;
                bootstrap_perform(
                    driver,
                    Sqe::listen_at(FdSlot::new(idx), backlog, BOOTSTRAP_UD),
                )?;
                Ok((idx, addr))
            }?;
            Ok((
                unsafe { Fd::from_raw_slot(FdSlot::new(idx), reference) },
                bound,
            ))
        }

        fn bind_datagram_slot<'d>(
            driver: &mut DriverContext<'_, 'd>,
            addr: SocketAddr,
        ) -> io::Result<(Fd<'d>, SocketAddr)> {
            let reference = driver.driver_ref();
            let config = ListenerConfig::for_datagram(&addr);
            let (idx, bound) = if addr.port() == 0 {
                bootstrap_bound_via_syscall(driver, addr, Kind::Dgram, &config, None)
            } else {
                let idx = bootstrap_bind_slot(
                    driver,
                    Domain::for_addr(&addr),
                    Kind::Dgram,
                    addr,
                    Some(&config),
                )?;
                Ok((idx, addr))
            }?;
            Ok((
                unsafe { Fd::from_raw_slot(FdSlot::new(idx), reference) },
                bound,
            ))
        }
    }

    fn bootstrap_await(
        driver: &mut DriverContext<'_, '_>,
        min: i32,
        fallback: i32,
    ) -> io::Result<()> {
        let rc = driver.backend().await_one()?;
        if rc < min {
            return Err(Error::from_raw_os_error(if rc < 0 {
                -rc
            } else {
                fallback
            }));
        }
        Ok(())
    }

    fn bootstrap_perform(driver: &mut DriverContext<'_, '_>, sqe: Sqe) -> io::Result<()> {
        Submission::push(driver, sqe)?;
        bootstrap_await(driver, 0, 0)
    }

    fn bootstrap_perform_raw(driver: &mut DriverContext<'_, '_>, sqe: RawSqe) -> io::Result<()> {
        // SAFETY: each caller waits for this exact operation before any
        // stack-backed address, fd array, or owned handle can leave scope.
        unsafe { crate::driver::submission::raw::Submission::push_raw(driver, sqe) }?;
        bootstrap_await(driver, 0, 0)
    }

    fn bootstrap_bind_slot(
        driver: &mut DriverContext<'_, '_>,
        domain: Domain,
        kind: Kind,
        addr: SocketAddr,
        config: Option<&ListenerConfig>,
    ) -> io::Result<u32> {
        let idx = driver.backend().alloc_fixed_range(1)?;
        let slot = FdSlot::new(idx);
        bootstrap_perform(
            driver,
            Sqe::socket_at(domain.raw(), kind.raw(), 0, slot, BOOTSTRAP_UD)?,
        )?;
        if let Some(config) = config {
            bootstrap_apply_config(driver, idx, config)?;
        }
        let bound = Addr::from_std(addr);
        bootstrap_perform_raw(
            driver,
            RawSqe::bind_at(slot, bound.ptr(), bound.socklen(), BOOTSTRAP_UD),
        )?;
        Ok(idx)
    }

    fn bootstrap_apply_config(
        driver: &mut DriverContext<'_, '_>,
        slot: u32,
        config: &ListenerConfig,
    ) -> io::Result<()> {
        if config.reuse_addr {
            bootstrap_setsockopt(driver, slot, SOL_SOCKET, SO_REUSEADDR, 1)?;
        }
        if config.reuse_port {
            bootstrap_setsockopt(driver, slot, SOL_SOCKET, SO_REUSEPORT, 1)?;
        }
        if let Some(qlen) = config.fast_open_backlog {
            bootstrap_setsockopt(driver, slot, IPPROTO_TCP, TCP_FASTOPEN, qlen)?;
        }
        if let Some(secs) = config.defer_accept_secs {
            bootstrap_setsockopt(driver, slot, IPPROTO_TCP, TCP_DEFER_ACCEPT, secs)?;
        }
        Ok(())
    }

    fn bootstrap_setsockopt(
        driver: &mut DriverContext<'_, '_>,
        slot: u32,
        level: i32,
        name: i32,
        value: i32,
    ) -> io::Result<()> {
        <Backend as ControlBackend>::submit_option(
            driver.backend(),
            FdSlot::new(slot),
            level,
            name,
            value,
        )?;
        bootstrap_await(driver, 0, 0)
    }

    fn bootstrap_register_raw(
        driver: &mut DriverContext<'_, '_>,
        raw: RawFd,
        slot: u32,
    ) -> io::Result<()> {
        let mut fds = [raw];
        let entry = FilesUpdate::new(fds.as_mut_ptr().cast_const(), 1)
            .offset(slot as i32)
            .build()
            .user_data(BOOTSTRAP_UD.raw());
        // SAFETY: `fds` remains live and unchanged through the synchronous
        // bootstrap completion immediately below.
        unsafe {
            crate::driver::submission::raw::Submission::push_raw(driver, RawSqe::from_entry(entry))
        }?;
        bootstrap_await(driver, 1, EMFILE)?;
        driver.backend().files.set_live(FdSlot::new(slot));
        Ok(())
    }

    fn bootstrap_bound_via_syscall(
        driver: &mut DriverContext<'_, '_>,
        addr: SocketAddr,
        kind: Kind,
        config: &ListenerConfig,
        backlog: Option<i32>,
    ) -> io::Result<(u32, SocketAddr)> {
        let handle = Handle::open(Domain::for_addr(&addr), kind)?;
        handle.apply_reuse(config)?;
        handle.bind(&Addr::from_std(addr))?;
        match backlog {
            Some(backlog) => handle.listen(backlog)?,
            None => handle.set_nonblocking()?,
        }
        let actual = handle.local_addr()?;
        let slot = driver.backend().alloc_fixed_range(1)?;
        bootstrap_register_raw(driver, handle.as_raw_fd(), slot)?;
        drop(handle);
        Ok((slot, actual))
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use std::os::fd::IntoRawFd;

    use crate::io::fd::FdSlot;
    use crate::io::ffi::Handle;
    use crate::io::socket::addr::Addr;
    use crate::io::socket::{Domain, Kind};

    use super::{Backend, BootstrapBackend, DriverContext, Fd, ListenerConfig, SocketAddr, io};

    impl BootstrapBackend for Backend {
        fn bind_listener_slot<'d>(
            driver: &mut DriverContext<'_, 'd>,
            addr: SocketAddr,
            backlog: i32,
            config: &ListenerConfig,
        ) -> io::Result<(Fd<'d>, SocketAddr)> {
            let reference = driver.driver_ref();
            let handle = Handle::open(Domain::for_addr(&addr), Kind::Stream)?;
            handle.apply_reuse(config)?;
            handle.bind(&Addr::from_std(addr))?;
            handle.listen(backlog)?;
            let actual = handle.local_addr()?;
            let idx = register(driver.backend(), handle)?;
            Ok((
                unsafe { Fd::from_raw_slot(FdSlot::new(idx), reference) },
                actual,
            ))
        }

        fn bind_datagram_slot<'d>(
            driver: &mut DriverContext<'_, 'd>,
            addr: SocketAddr,
        ) -> io::Result<(Fd<'d>, SocketAddr)> {
            let reference = driver.driver_ref();
            let handle = Handle::open(Domain::for_addr(&addr), Kind::Dgram)?;
            handle.set_nonblocking()?;
            handle.apply_reuse(&ListenerConfig::for_datagram(&addr))?;
            handle.bind(&Addr::from_std(addr))?;
            let actual = handle.local_addr()?;
            let idx = register(driver.backend(), handle)?;
            Ok((
                unsafe { Fd::from_raw_slot(FdSlot::new(idx), reference) },
                actual,
            ))
        }
    }

    fn register(backend: &mut Backend, handle: Handle) -> io::Result<u32> {
        let slot = backend.alloc_fixed_range(1)?;
        backend.register_raw_fd(slot, handle.into_raw_fd())?;
        Ok(slot)
    }
}
