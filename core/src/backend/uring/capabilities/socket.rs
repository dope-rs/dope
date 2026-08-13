use std::{io, net};

use crate::{
    backend::{self, bound},
    driver::{
        self, flight,
        route::{self, kind},
    },
    io::{
        fd::handles,
        socket::{establishment, option},
    },
    platform::reactor,
};

impl backend::Socket for backend::Uring {
    const MAX_IOVECS: usize = libc::UIO_MAXIOV as usize;
    const KEEP_ALIVE_IDLE: libc::c_int = libc::TCP_KEEPIDLE;
    const KEEP_ALIVE_INTERVAL: libc::c_int = libc::TCP_KEEPINTVL;
    const KEEP_ALIVE_RETRIES: libc::c_int = libc::TCP_KEEPCNT;

    fn encode_v4(addr: net::SocketAddrV4) -> libc::sockaddr_in {
        use libc::{AF_INET, in_addr, sockaddr_in};
        sockaddr_in {
            sin_family: AF_INET as _,
            sin_port: addr.port().to_be(),
            sin_addr: in_addr {
                s_addr: u32::from_ne_bytes(addr.ip().octets()),
            },
            sin_zero: [0; 8],
        }
    }

    fn encode_v6(addr: net::SocketAddrV6) -> libc::sockaddr_in6 {
        use libc::{AF_INET6, in6_addr, sockaddr_in6};
        sockaddr_in6 {
            sin6_family: AF_INET6 as _,
            sin6_port: addr.port().to_be(),
            sin6_flowinfo: addr.flowinfo(),
            sin6_addr: in6_addr {
                s6_addr: addr.ip().octets(),
            },
            sin6_scope_id: addr.scope_id(),
        }
    }

    fn encode_unix(bytes: &[u8]) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
        use libc::sockaddr_un;
        if bytes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty path"));
        }
        let mut encoded = sockaddr_un {
            sun_family: libc::AF_UNIX as _,
            sun_path: [0; 108],
        };
        let Some(max) = encoded.sun_path.len().checked_sub(1) else {
            use std::process::abort;
            abort();
        };
        if bytes.len() > max {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "path too long"));
        }
        for (index, byte) in bytes.iter().enumerate() {
            encoded.sun_path[index] = *byte as libc::c_char;
        }
        let len = (size_of::<libc::sa_family_t>() + bytes.len() + 1) as libc::socklen_t;
        Ok((encoded, len))
    }

    fn submit_tuning<'d, Tag: route::Tag>(
        &mut self,
        target: route::Target<'d, Tag>,
        fd: handles::Descriptor<'d>,
        options: option::StreamOptions,
    ) -> Result<option::Tuning<'d>, handles::Descriptor<'d>> {
        if options.is_empty() {
            return Ok(option::Tuning::Applied(fd));
        }
        let token = route::Token::from_target(target).with_kind(kind::TUNING);
        let backend::Uring { ring, tuning, .. } = self;
        match tuning.submit_tuning(ring, &fd, options, token) {
            Ok(()) => Ok(option::Tuning::Pending(
                establishment::TuningPending::tuning(fd, target),
            )),
            Err(_) => Err(fd),
        }
    }

    fn submit_tuned_connect<'owner, 'd: 'owner, Tag: route::Tag>(
        &mut self,
        flights: &flight::Slots<'d, Tag>,
        options: option::StreamOptions,
        connect: backend::raw::RetainedConnect<'owner, 'd, Tag>,
    ) -> Result<establishment::ConnectionPending<'d>, handles::Descriptor<'d>> {
        let (fd, terminal, target) = connect.into_parts();
        let token = route::Token::from_target(target).with_kind(kind::CONNECT);
        if options.is_empty() {
            let Some(terminal) =
                bound::Bound::reserve_retained(terminal, target.operation(kind::CONNECT), flights)
            else {
                return Err(fd);
            };
            return match reactor::Queue::submit(&mut reactor::Source::queue(self), terminal) {
                Ok(flight) => Ok(establishment::ConnectionPending::connect(fd, flight)),
                Err(_) => Err(fd),
            };
        }
        let terminal = terminal.map(|terminal| terminal.into_inner().into_entry());
        let backend::Uring { ring, tuning, .. } = self;
        match tuning.submit_tuned_connect(ring, &fd, options, token, terminal) {
            Ok(()) => Ok(establishment::ConnectionPending::tuned_connect(fd, target)),
            Err(_) => Err(fd),
        }
    }

    fn cancel_establishment(
        &mut self,
        target: establishment::CancelTarget<'_, '_>,
    ) -> Result<(), driver::SubmitError> {
        match target {
            establishment::CancelTarget::Connect(flight) => {
                reactor::Queue::cancel(&mut reactor::Source::queue(self), flight)
            }
            establishment::CancelTarget::Tuning(fd) => {
                let backend::Uring { ring, tuning, .. } = self;
                tuning.cancel(ring, fd.token_index())
            }
        }
    }
}
