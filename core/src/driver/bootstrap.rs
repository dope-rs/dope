use std::io;
use std::net::SocketAddr;

use super::DriverContext;
use crate::io::fd::{Fd, FdSlot};
use crate::io::ffi::Handle;
use crate::io::socket::addr::Addr;
use crate::io::socket::{Domain, Kind, ListenerConfig};

pub trait Bootstrap<'d> {
    fn bind_listener_slot(
        &mut self,
        addr: SocketAddr,
        backlog: i32,
        config: &ListenerConfig,
    ) -> io::Result<(Fd<'d>, SocketAddr)>;
    fn bind_datagram_slot(&mut self, addr: SocketAddr) -> io::Result<(Fd<'d>, SocketAddr)>;
}

cfg_select! {
    target_os = "linux" => {
        use std::io::Error;
        use std::os::fd::{AsRawFd, RawFd};

        use io_uring::opcode::FilesUpdate;

        use super::control::ContextControl;
        use super::submission::Submission;
        use super::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token};
        use crate::backend::uring::sqe::{self, Sqe};

        const BOOTSTRAP_UD: Token = Token::new(ROUTE_FRAMEWORK, SlotIndex::new(0), Epoch::ZERO);

        impl<'a, 'd> Bootstrap<'d> for DriverContext<'a, 'd> {
            fn bind_listener_slot(
                &mut self,
                addr: SocketAddr,
                backlog: i32,
                config: &ListenerConfig,
            ) -> io::Result<(Fd<'d>, SocketAddr)> {
                let reference = self.driver_ref();
                let (idx, bound) = if addr.port() == 0 {
                    bootstrap_bound_via_syscall(self, addr, Kind::Stream, config, Some(backlog))
                } else {
                    let idx = bootstrap_bind_slot(
                        self,
                        Domain::for_addr(&addr),
                        Kind::Stream,
                        addr,
                        Some(config),
                    )?;
                    bootstrap_perform(
                        self,
                        Sqe::listen_at(FdSlot::new(idx), backlog, BOOTSTRAP_UD),
                    )?;
                    Ok((idx, addr))
                }?;
                Ok((
                    unsafe { Fd::from_raw_slot(FdSlot::new(idx), reference) },
                    bound,
                ))
            }

            fn bind_datagram_slot(&mut self, addr: SocketAddr) -> io::Result<(Fd<'d>, SocketAddr)> {
                let reference = self.driver_ref();
                let config = ListenerConfig::for_datagram(&addr);
                let (idx, bound) = if addr.port() == 0 {
                    bootstrap_bound_via_syscall(self, addr, Kind::Dgram, &config, None)
                } else {
                    let idx = bootstrap_bind_slot(
                        self,
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

        fn bootstrap_perform(driver: &mut DriverContext<'_, '_>, sqe: sqe::Sqe) -> io::Result<()> {
            Submission::push(driver, sqe)?;
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
            bootstrap_perform(
                driver,
                Sqe::bind_at(slot, bound.ptr(), bound.socklen(), BOOTSTRAP_UD),
            )?;
            Ok(idx)
        }

        fn bootstrap_apply_config(
            driver: &mut DriverContext<'_, '_>,
            slot: u32,
            config: &ListenerConfig,
        ) -> io::Result<()> {
            if config.reuse_addr {
                bootstrap_setsockopt(
                    driver,
                    slot,
                    libc::SOL_SOCKET as u32,
                    libc::SO_REUSEADDR as u32,
                    1,
                )?;
            }
            if config.reuse_port {
                bootstrap_setsockopt(
                    driver,
                    slot,
                    libc::SOL_SOCKET as u32,
                    libc::SO_REUSEPORT as u32,
                    1,
                )?;
            }
            if let Some(qlen) = config.fast_open_backlog {
                bootstrap_setsockopt(
                    driver,
                    slot,
                    libc::IPPROTO_TCP as u32,
                    libc::TCP_FASTOPEN as u32,
                    qlen as i32,
                )?;
            }
            if let Some(secs) = config.defer_accept_secs {
                bootstrap_setsockopt(
                    driver,
                    slot,
                    libc::IPPROTO_TCP as u32,
                    libc::TCP_DEFER_ACCEPT as u32,
                    secs as i32,
                )?;
            }
            Ok(())
        }

        fn bootstrap_setsockopt(
            driver: &mut DriverContext<'_, '_>,
            slot: u32,
            level: u32,
            optname: u32,
            value: i32,
        ) -> io::Result<()> {
            ContextControl::set(driver, slot, level, optname, value)?;
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
            Submission::push(driver, Sqe::from_entry(entry))?;
            bootstrap_await(driver, 1, libc::EMFILE)?;
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
    _ => {
        use std::os::fd::IntoRawFd;

        use crate::backend::kqueue::driver::Kqueue;

        impl<'a, 'd> Bootstrap<'d> for DriverContext<'a, 'd> {
            fn bind_listener_slot(
                &mut self,
                addr: SocketAddr,
                backlog: i32,
                config: &ListenerConfig,
            ) -> io::Result<(Fd<'d>, SocketAddr)> {
                let reference = self.driver_ref();
                let state = self.backend();
                let handle = Handle::open(Domain::for_addr(&addr), Kind::Stream)?;
                handle.apply_reuse(config)?;
                handle.bind(&Addr::from_std(addr))?;
                handle.listen(backlog)?;
                let actual = handle.local_addr()?;
                let idx = register(state, handle)?;
                Ok((
                    unsafe { Fd::from_raw_slot(FdSlot::new(idx), reference) },
                    actual,
                ))
            }

            fn bind_datagram_slot(
                &mut self,
                addr: SocketAddr,
            ) -> io::Result<(Fd<'d>, SocketAddr)> {
                let reference = self.driver_ref();
                let state = self.backend();
                let handle = Handle::open(Domain::for_addr(&addr), Kind::Dgram)?;
                handle.set_nonblocking()?;
                handle.apply_reuse(&ListenerConfig::for_datagram(&addr))?;
                handle.bind(&Addr::from_std(addr))?;
                let actual = handle.local_addr()?;
                let idx = register(state, handle)?;
                Ok((
                    unsafe { Fd::from_raw_slot(FdSlot::new(idx), reference) },
                    actual,
                ))
            }
        }

        fn register(state: &mut Kqueue, handle: Handle) -> io::Result<u32> {
            let slot = state.alloc_fixed_range(1)?;
            state.register_raw_fd(slot, handle.into_raw_fd())?;
            Ok(slot)
        }
    }
}
