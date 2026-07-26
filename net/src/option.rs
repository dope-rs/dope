use std::time::Duration;

use dope_core::driver::DriverContext;
use dope_core::driver::control::ContextControl;
use dope_core::io::fd::Fd;
use libc::IPPROTO_TCP;
use libc::SO_KEEPALIVE;
use libc::SO_RCVBUF;
use libc::SO_SNDBUF;
use libc::SOL_SOCKET;
use libc::TCP_NODELAY;
use libc::c_int;

const TCP_KEEPIDLE: c_int = 4;
const TCP_KEEPINTVL: c_int = 5;
const TCP_KEEPCNT: c_int = 6;
const TCP_QUICKACK: c_int = 12;
const TCP_USER_TIMEOUT: c_int = 18;

#[derive(Clone, Copy)]
pub(super) enum SocketOption {
    KeepAlive(bool),
    RecvBuffer(usize),
    SendBuffer(usize),
    NoDelay(bool),
    QuickAck(bool),
    KeepAliveIdle(Duration),
    KeepAliveInterval(Duration),
    KeepAliveRetries(u32),
    UserTimeout(Duration),
}

impl SocketOption {
    fn resolve(self) -> Option<(c_int, c_int, c_int)> {
        let (level, opt, raw): (c_int, c_int, u128) = match self {
            Self::KeepAlive(on) => (SOL_SOCKET, SO_KEEPALIVE, on as u128),
            Self::RecvBuffer(size) => (SOL_SOCKET, SO_RCVBUF, size as u128),
            Self::SendBuffer(size) => (SOL_SOCKET, SO_SNDBUF, size as u128),
            Self::NoDelay(on) => (IPPROTO_TCP, TCP_NODELAY, on as u128),
            Self::QuickAck(on) => (IPPROTO_TCP, TCP_QUICKACK, on as u128),
            Self::KeepAliveIdle(duration) => {
                (IPPROTO_TCP, TCP_KEEPIDLE, duration.as_secs() as u128)
            }
            Self::KeepAliveInterval(duration) => {
                (IPPROTO_TCP, TCP_KEEPINTVL, duration.as_secs() as u128)
            }
            Self::KeepAliveRetries(retries) => (IPPROTO_TCP, TCP_KEEPCNT, retries as u128),
            Self::UserTimeout(d) => (IPPROTO_TCP, TCP_USER_TIMEOUT, d.as_millis()),
        };
        let value = c_int::try_from(raw).ok()?;
        Some((level, opt, value))
    }

    fn submit(self, driver: &mut DriverContext<'_, '_>, fd: &Fd<'_>) {
        if let Some((level, opt, value)) = self.resolve() {
            let _ = driver.set(fd.index(), level as u32, opt as u32, value);
        }
    }

    pub(super) fn submit_all<const N: usize>(
        options: [Option<SocketOption>; N],
        driver: &mut DriverContext<'_, '_>,
        fd: &Fd<'_>,
    ) {
        for option in options.into_iter().flatten() {
            option.submit(driver, fd);
        }
    }
}
