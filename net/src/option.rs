use std::io;
use std::io::{Error, ErrorKind};
use std::time::Duration;

use dope_core::driver::DriverContext;
use dope_core::driver::control::ContextControl;
use dope_core::io::fd::Fd;
use dope_core::io::socket::option::SocketOption;
use libc::c_int;

#[cfg(target_os = "linux")]
mod platform {
    use libc::c_int;

    pub(super) const KEEP_ALIVE_IDLE: Option<c_int> = Some(libc::TCP_KEEPIDLE);
    pub(super) const KEEP_ALIVE_INTERVAL: Option<c_int> = Some(libc::TCP_KEEPINTVL);
    pub(super) const KEEP_ALIVE_RETRIES: Option<c_int> = Some(libc::TCP_KEEPCNT);
    pub(super) const QUICK_ACK: Option<c_int> = Some(libc::TCP_QUICKACK);
    pub(super) const USER_TIMEOUT: Option<c_int> = Some(libc::TCP_USER_TIMEOUT);
}

#[cfg(target_os = "macos")]
mod platform {
    use libc::c_int;

    pub(super) const KEEP_ALIVE_IDLE: Option<c_int> = Some(libc::TCP_KEEPALIVE);
    pub(super) const KEEP_ALIVE_INTERVAL: Option<c_int> = Some(libc::TCP_KEEPINTVL);
    pub(super) const KEEP_ALIVE_RETRIES: Option<c_int> = Some(libc::TCP_KEEPCNT);
    pub(super) const QUICK_ACK: Option<c_int> = None;
    pub(super) const USER_TIMEOUT: Option<c_int> = None;
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use libc::c_int;

    pub(super) const KEEP_ALIVE_IDLE: Option<c_int> = None;
    pub(super) const KEEP_ALIVE_INTERVAL: Option<c_int> = None;
    pub(super) const KEEP_ALIVE_RETRIES: Option<c_int> = None;
    pub(super) const QUICK_ACK: Option<c_int> = None;
    pub(super) const USER_TIMEOUT: Option<c_int> = None;
}

#[derive(Clone, Copy)]
pub(super) enum StreamOption {
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

impl StreamOption {
    fn positive_raw(raw: u128) -> Option<c_int> {
        if raw == 0 {
            None
        } else {
            c_int::try_from(raw).ok()
        }
    }

    pub(super) fn seconds_raw(duration: Duration) -> Option<c_int> {
        let seconds = duration
            .as_secs()
            .checked_add(u64::from(duration.subsec_nanos() != 0))?;
        Self::positive_raw(seconds as u128)
    }

    fn milliseconds_raw(duration: Duration) -> Option<c_int> {
        let milliseconds = duration.as_millis().checked_add(u128::from(
            !duration.subsec_nanos().is_multiple_of(1_000_000),
        ))?;
        c_int::try_from(milliseconds).ok()
    }

    fn resolve(self) -> Option<SocketOption> {
        let (level, name, value) = match self {
            Self::KeepAlive(on) => (libc::SOL_SOCKET, libc::SO_KEEPALIVE, on.into()),
            Self::RecvBuffer(size) => (
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                c_int::try_from(size).ok()?,
            ),
            Self::SendBuffer(size) => (
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                c_int::try_from(size).ok()?,
            ),
            Self::NoDelay(on) => (libc::IPPROTO_TCP, libc::TCP_NODELAY, on.into()),
            Self::QuickAck(on) => (libc::IPPROTO_TCP, platform::QUICK_ACK?, on.into()),
            Self::KeepAliveIdle(duration) => (
                libc::IPPROTO_TCP,
                platform::KEEP_ALIVE_IDLE?,
                Self::seconds_raw(duration)?,
            ),
            Self::KeepAliveInterval(duration) => (
                libc::IPPROTO_TCP,
                platform::KEEP_ALIVE_INTERVAL?,
                Self::seconds_raw(duration)?,
            ),
            Self::KeepAliveRetries(retries) => (
                libc::IPPROTO_TCP,
                platform::KEEP_ALIVE_RETRIES?,
                Self::positive_raw(retries.into())?,
            ),
            Self::UserTimeout(duration) => (
                libc::IPPROTO_TCP,
                platform::USER_TIMEOUT?,
                Self::milliseconds_raw(duration)?,
            ),
        };
        Some(SocketOption::new(level, name, value))
    }

    fn validate(self) -> io::Result<()> {
        if self.resolve().is_some() {
            return Ok(());
        }
        let (kind, message) = match self {
            Self::RecvBuffer(_) => (ErrorKind::InvalidInput, "receive buffer size exceeds c_int"),
            Self::SendBuffer(_) => (ErrorKind::InvalidInput, "send buffer size exceeds c_int"),
            Self::QuickAck(_) => (ErrorKind::Unsupported, "TCP_QUICKACK is unsupported"),
            Self::KeepAliveIdle(_) if platform::KEEP_ALIVE_IDLE.is_none() => {
                (ErrorKind::Unsupported, "TCP keep-alive idle is unsupported")
            }
            Self::KeepAliveIdle(_) => (
                ErrorKind::InvalidInput,
                "keep-alive idle must fit positive c_int seconds",
            ),
            Self::KeepAliveInterval(_) if platform::KEEP_ALIVE_INTERVAL.is_none() => (
                ErrorKind::Unsupported,
                "TCP keep-alive interval is unsupported",
            ),
            Self::KeepAliveInterval(_) => (
                ErrorKind::InvalidInput,
                "keep-alive interval must fit positive c_int seconds",
            ),
            Self::KeepAliveRetries(_) if platform::KEEP_ALIVE_RETRIES.is_none() => (
                ErrorKind::Unsupported,
                "TCP keep-alive retries are unsupported",
            ),
            Self::KeepAliveRetries(_) => (
                ErrorKind::InvalidInput,
                "keep-alive retries must fit a positive c_int",
            ),
            Self::UserTimeout(_) if platform::USER_TIMEOUT.is_none() => {
                (ErrorKind::Unsupported, "TCP_USER_TIMEOUT is unsupported")
            }
            Self::UserTimeout(_) => (
                ErrorKind::InvalidInput,
                "user timeout must fit c_int milliseconds",
            ),
            Self::KeepAlive(_) | Self::NoDelay(_) => return Ok(()),
        };
        Err(Error::new(kind, message))
    }

    pub(super) fn supports_user_timeout() -> bool {
        platform::USER_TIMEOUT.is_some()
    }

    pub(super) fn validate_all<const N: usize>(
        options: [Option<StreamOption>; N],
    ) -> io::Result<()> {
        for option in options.into_iter().flatten() {
            option.validate()?;
        }
        Ok(())
    }

    pub(super) fn submit(
        option: Option<StreamOption>,
        driver: &mut DriverContext<'_, '_>,
        fd: &Fd<'_>,
    ) -> bool {
        let Some(option) = option else {
            return true;
        };
        let Some(option) = option.resolve() else {
            return false;
        };
        driver.submit_option(fd, option).is_ok()
    }
}
