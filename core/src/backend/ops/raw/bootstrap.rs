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

    use super::{Backend, BootstrapBackend, DriverContext, Fd, ListenerConfig, SocketAddr, io};
    use crate::backend::ops::raw::control::ControlBackend;
    use crate::backend::{RawSqe, RetainedSqe, Sqe, StableSqeSource};
    use crate::driver::submission::Submission;
    use crate::driver::token::{SlotIndex, Token};
    use crate::io::fd::FdSlot;
    use crate::io::ffi::Handle;
    use crate::io::socket::addr::Addr;
    use crate::io::socket::{Domain, Kind};

    const BOOTSTRAP_UD: Token = Token::framework(SlotIndex::ZERO);

    struct SynchronousSqe(RawSqe);

    // SAFETY: the sole consumer submits and awaits the terminal completion
    // before returning to the scope that owns the captured resources.
    unsafe impl StableSqeSource for SynchronousSqe {
        fn into_raw(self) -> RawSqe {
            self.0
        }
    }

    struct FixedSlotReservation<'a, 'c, 'd> {
        driver: &'a mut DriverContext<'c, 'd>,
        slot: FdSlot,
        committed: bool,
    }

    impl<'a, 'c, 'd> FixedSlotReservation<'a, 'c, 'd> {
        fn reserve(driver: &'a mut DriverContext<'c, 'd>) -> io::Result<Self> {
            let slot = driver.backend().alloc_fixed_slot()?;
            Ok(Self {
                driver,
                slot,
                committed: false,
            })
        }

        fn driver(&mut self) -> &mut DriverContext<'c, 'd> {
            self.driver
        }

        fn slot(&self) -> FdSlot {
            self.slot
        }

        fn commit(mut self) -> FdSlot {
            self.committed = true;
            self.slot
        }
    }

    impl Drop for FixedSlotReservation<'_, '_, '_> {
        fn drop(&mut self) {
            if self.committed {
                return;
            }
            self.driver.backend().close_fd(self.slot);
            self.driver.backend().retire_fixed_range(self.slot.raw(), 1);
        }
    }

    impl BootstrapBackend for Backend {
        fn bind_listener_slot<'d>(
            driver: &mut DriverContext<'_, 'd>,
            addr: SocketAddr,
            backlog: i32,
            config: &ListenerConfig,
        ) -> io::Result<(Fd<'d>, SocketAddr)> {
            let reference = driver.driver_ref();
            let (slot, bound) = if addr.port() == 0 {
                bootstrap_bound_via_syscall(driver, addr, Kind::Stream, config, Some(backlog))
            } else {
                let slot = bootstrap_bind_slot(
                    driver,
                    Domain::for_addr(&addr),
                    Kind::Stream,
                    addr,
                    Some(config),
                )?;
                bootstrap_perform(driver, Sqe::listen_at(slot, backlog, BOOTSTRAP_UD))?;
                Ok((slot, addr))
            }?;
            Ok((Fd::from_reserved_slot(slot, reference), bound))
        }

        fn bind_datagram_slot<'d>(
            driver: &mut DriverContext<'_, 'd>,
            addr: SocketAddr,
        ) -> io::Result<(Fd<'d>, SocketAddr)> {
            let reference = driver.driver_ref();
            let config = ListenerConfig::for_datagram(&addr);
            let (slot, bound) = if addr.port() == 0 {
                bootstrap_bound_via_syscall(driver, addr, Kind::Dgram, &config, None)
            } else {
                let slot = bootstrap_bind_slot(
                    driver,
                    Domain::for_addr(&addr),
                    Kind::Dgram,
                    addr,
                    Some(&config),
                )?;
                Ok((slot, addr))
            }?;
            Ok((Fd::from_reserved_slot(slot, reference), bound))
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

    fn bootstrap_perform_raw(
        driver: &mut DriverContext<'_, '_>,
        sqe: RawSqe,
        min: i32,
        fallback: i32,
    ) -> io::Result<()> {
        Submission::push_retained(driver, RetainedSqe::from_stable(SynchronousSqe(sqe)))?;
        bootstrap_await(driver, min, fallback)
    }

    fn bootstrap_bind_slot(
        driver: &mut DriverContext<'_, '_>,
        domain: Domain,
        kind: Kind,
        addr: SocketAddr,
        config: Option<&ListenerConfig>,
    ) -> io::Result<FdSlot> {
        let mut reservation = FixedSlotReservation::reserve(driver)?;
        let slot = reservation.slot();
        bootstrap_perform(
            reservation.driver(),
            Sqe::socket_at(domain.raw(), kind.raw(), 0, slot, BOOTSTRAP_UD)?,
        )?;
        if let Some(config) = config {
            bootstrap_apply_config(reservation.driver(), slot, config)?;
        }
        let bound = Addr::from_std(addr);
        bootstrap_perform_raw(
            reservation.driver(),
            RawSqe::bind_at(slot, bound.ptr(), bound.socklen(), BOOTSTRAP_UD),
            0,
            0,
        )?;
        Ok(reservation.commit())
    }

    fn bootstrap_apply_config(
        driver: &mut DriverContext<'_, '_>,
        slot: FdSlot,
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
        slot: FdSlot,
        level: i32,
        name: i32,
        value: i32,
    ) -> io::Result<()> {
        <Backend as ControlBackend>::submit_option(driver.backend(), slot, level, name, value)?;
        bootstrap_await(driver, 0, 0)
    }

    fn bootstrap_register_raw(
        driver: &mut DriverContext<'_, '_>,
        raw: RawFd,
        slot: FdSlot,
    ) -> io::Result<()> {
        driver.backend().await_fixed_range_empty(slot.raw(), 1)?;
        let mut fds = [raw];
        let entry = FilesUpdate::new(fds.as_mut_ptr().cast_const(), 1)
            .offset(slot.raw() as i32)
            .build()
            .user_data(BOOTSTRAP_UD.raw());
        bootstrap_perform_raw(driver, RawSqe::from_entry(entry), 1, EMFILE)?;
        driver.backend().files.set_live(slot);
        Ok(())
    }

    fn bootstrap_bound_via_syscall(
        driver: &mut DriverContext<'_, '_>,
        addr: SocketAddr,
        kind: Kind,
        config: &ListenerConfig,
        backlog: Option<i32>,
    ) -> io::Result<(FdSlot, SocketAddr)> {
        let handle = Handle::open(Domain::for_addr(&addr), kind)?;
        handle.apply_reuse(config)?;
        handle.bind(&Addr::from_std(addr))?;
        match backlog {
            Some(backlog) => handle.listen(backlog)?,
            None => handle.set_nonblocking()?,
        }
        let actual = handle.local_addr()?;
        let mut reservation = FixedSlotReservation::reserve(driver)?;
        let slot = reservation.slot();
        bootstrap_register_raw(reservation.driver(), handle.as_raw_fd(), slot)?;
        drop(handle);
        Ok((reservation.commit(), actual))
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use super::{Backend, BootstrapBackend, DriverContext, Fd, ListenerConfig, SocketAddr, io};
    use crate::io::fd::FdSlot;
    use crate::io::ffi::Handle;
    use crate::io::socket::addr::Addr;
    use crate::io::socket::{Domain, Kind};

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
            let slot = register(driver.backend(), handle)?;
            Ok((Fd::from_reserved_slot(slot, reference), actual))
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
            let slot = register(driver.backend(), handle)?;
            Ok((Fd::from_reserved_slot(slot, reference), actual))
        }
    }

    fn register(backend: &mut Backend, handle: Handle) -> io::Result<FdSlot> {
        let slot = backend.alloc_fixed_slot()?;
        backend.register_fd(slot.raw(), handle.into_owned());
        Ok(slot)
    }
}
