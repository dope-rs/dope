use std::time::Duration;

use dope_core::driver::DriverContext;
use dope_core::driver::control::ContextControl;
use libc::c_int;

const TCP_KEEPIDLE: libc::c_int = 4;
const TCP_KEEPINTVL: libc::c_int = 5;
const TCP_KEEPCNT: libc::c_int = 6;
const TCP_QUICKACK: libc::c_int = 12;
const TCP_USER_TIMEOUT: libc::c_int = 18;

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
    fn resolve(self) -> Option<(libc::c_int, libc::c_int, libc::c_int)> {
        let (level, opt, raw): (libc::c_int, libc::c_int, u128) = match self {
            Self::KeepAlive(on) => (libc::SOL_SOCKET, libc::SO_KEEPALIVE, on as u128),
            Self::RecvBuffer(size) => (libc::SOL_SOCKET, libc::SO_RCVBUF, size as u128),
            Self::SendBuffer(size) => (libc::SOL_SOCKET, libc::SO_SNDBUF, size as u128),
            Self::NoDelay(on) => (libc::IPPROTO_TCP, libc::TCP_NODELAY, on as u128),
            Self::QuickAck(on) => (libc::IPPROTO_TCP, TCP_QUICKACK, on as u128),
            Self::KeepAliveIdle(duration) => {
                (libc::IPPROTO_TCP, TCP_KEEPIDLE, duration.as_secs() as u128)
            }
            Self::KeepAliveInterval(duration) => {
                (libc::IPPROTO_TCP, TCP_KEEPINTVL, duration.as_secs() as u128)
            }
            Self::KeepAliveRetries(retries) => (libc::IPPROTO_TCP, TCP_KEEPCNT, retries as u128),
            Self::UserTimeout(d) => (libc::IPPROTO_TCP, TCP_USER_TIMEOUT, d.as_millis()),
        };
        let value = c_int::try_from(raw).ok()?;
        Some((level, opt, value))
    }

    fn submit(self, driver: &mut DriverContext<'_, '_>, fd: &dope_core::io::fd::Fd<'_>) {
        if let Some((level, opt, value)) = self.resolve() {
            let _ = driver.set(fd.index(), level as u32, opt as u32, value);
        }
    }

    pub(super) fn submit_all<const N: usize>(
        options: [Option<SocketOption>; N],
        driver: &mut DriverContext<'_, '_>,
        fd: &dope_core::io::fd::Fd<'_>,
    ) {
        for option in options.into_iter().flatten() {
            option.submit(driver, fd);
        }
    }
}
