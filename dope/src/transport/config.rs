use std::time::Duration;

use crate::{Driver, Sockopt};

const TCP_KEEPIDLE: libc::c_int = 4;
const TCP_KEEPINTVL: libc::c_int = 5;
const TCP_KEEPCNT: libc::c_int = 6;
const TCP_QUICKACK: libc::c_int = 12;
const TCP_USER_TIMEOUT: libc::c_int = 18;

#[derive(Clone, Copy)]
enum SockOpt {
    Keepalive(bool),
    RecvBuf(usize),
    SendBuf(usize),
    Nodelay(bool),
    Quickack(bool),
    KeepaliveIdle(Duration),
    KeepaliveIntvl(Duration),
    KeepaliveCnt(u32),
    UserTimeout(Duration),
}

impl SockOpt {
    fn resolve(self) -> Option<(libc::c_int, libc::c_int, libc::c_int)> {
        let (level, opt, raw): (libc::c_int, libc::c_int, u128) = match self {
            Self::Keepalive(on) => (libc::SOL_SOCKET, libc::SO_KEEPALIVE, on as u128),
            Self::RecvBuf(size) => (libc::SOL_SOCKET, libc::SO_RCVBUF, size as u128),
            Self::SendBuf(size) => (libc::SOL_SOCKET, libc::SO_SNDBUF, size as u128),
            Self::Nodelay(on) => (libc::IPPROTO_TCP, libc::TCP_NODELAY, on as u128),
            Self::Quickack(on) => (libc::IPPROTO_TCP, TCP_QUICKACK, on as u128),
            Self::KeepaliveIdle(d) => (libc::IPPROTO_TCP, TCP_KEEPIDLE, d.as_secs() as u128),
            Self::KeepaliveIntvl(d) => (libc::IPPROTO_TCP, TCP_KEEPINTVL, d.as_secs() as u128),
            Self::KeepaliveCnt(cnt) => (libc::IPPROTO_TCP, TCP_KEEPCNT, cnt as u128),
            Self::UserTimeout(d) => (libc::IPPROTO_TCP, TCP_USER_TIMEOUT, d.as_millis()),
        };
        let value = libc::c_int::try_from(raw).ok()?;
        Some((level, opt, value))
    }

    fn submit(self, idx: u32, driver: &Driver) {
        if let Some((level, opt, value)) = self.resolve() {
            let _ = driver.set(idx, level as u32, opt as u32, value);
        }
    }
}

pub trait Submittable {
    fn submit(self, idx: u32, driver: &Driver);
}

#[derive(Clone, Copy, Default)]
pub enum SocketToggle {
    #[default]
    Inherit,
    Enabled,
}

impl SocketToggle {
    pub(super) const fn flag(self) -> Option<bool> {
        match self {
            Self::Inherit => None,
            Self::Enabled => Some(true),
        }
    }
}

pub mod tcp {

    use std::time::Duration;

    use super::{SockOpt, SocketToggle, Submittable};
    use crate::Driver;

    #[derive(Clone, Copy, Default)]
    pub struct StreamOpts {
        pub recv_buffer_size: Option<usize>,
        pub send_buffer_size: Option<usize>,
        pub quickack: SocketToggle,
        pub nodelay: SocketToggle,
        pub keepalive: SocketToggle,
        pub keepalive_idle: Option<Duration>,
        pub keepalive_interval: Option<Duration>,
        pub keepalive_retries: Option<u32>,
        pub user_timeout: Option<Duration>,
    }

    impl StreamOpts {
        const fn keepalive_tuned(self) -> bool {
            self.keepalive_idle.is_some()
                || self.keepalive_interval.is_some()
                || self.keepalive_retries.is_some()
        }
    }

    impl Submittable for StreamOpts {
        fn submit(self, idx: u32, driver: &Driver) {
            let tuned = self.keepalive_tuned();
            let opts: [Option<SockOpt>; 9] = [
                self.quickack.flag().map(SockOpt::Quickack),
                self.nodelay.flag().map(SockOpt::Nodelay),
                self.keepalive.flag().map(SockOpt::Keepalive),
                self.recv_buffer_size.map(SockOpt::RecvBuf),
                self.send_buffer_size.map(SockOpt::SendBuf),
                self.keepalive_idle
                    .filter(|_| tuned)
                    .map(SockOpt::KeepaliveIdle),
                self.keepalive_interval
                    .filter(|_| tuned)
                    .map(SockOpt::KeepaliveIntvl),
                self.keepalive_retries
                    .filter(|_| tuned)
                    .map(SockOpt::KeepaliveCnt),
                self.user_timeout.map(SockOpt::UserTimeout),
            ];
            for opt in opts.into_iter().flatten() {
                opt.submit(idx, driver);
            }
        }
    }

    #[derive(Clone, Copy, Default)]
    pub struct ListenerOpts {
        pub reuseport: SocketToggle,
        pub fastopen_backlog: Option<u32>,
        pub defer_accept_secs: Option<u32>,
        pub per_ip_cap: Option<u32>,
    }
}

pub mod unix {

    use super::{SockOpt, Submittable};
    use crate::Driver;

    #[derive(Clone, Copy, Default)]
    pub struct StreamOpts {
        pub recv_buffer_size: Option<usize>,
        pub send_buffer_size: Option<usize>,
    }

    impl Submittable for StreamOpts {
        fn submit(self, idx: u32, driver: &Driver) {
            let opts: [Option<SockOpt>; 2] = [
                self.recv_buffer_size.map(SockOpt::RecvBuf),
                self.send_buffer_size.map(SockOpt::SendBuf),
            ];
            for opt in opts.into_iter().flatten() {
                opt.submit(idx, driver);
            }
        }
    }

    #[derive(Clone, Copy, Default)]
    pub struct ListenerOpts;
}
