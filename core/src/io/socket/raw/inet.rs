use std::net;

use crate::backend;

#[derive(Clone, Copy)]
#[repr(C)]
union InetStorage {
    v4: libc::sockaddr_in,
    v6: libc::sockaddr_in6,
}

#[derive(Clone, Copy)]
pub struct Inet {
    storage: InetStorage,
    len: libc::socklen_t,
}

impl Inet {
    pub fn from_std(addr: net::SocketAddr) -> Self {
        match addr {
            net::SocketAddr::V4(v4) => Self {
                storage: InetStorage {
                    v4: <backend::Backend as backend::Socket>::encode_v4(v4),
                },
                len: size_of::<libc::sockaddr_in>() as libc::socklen_t,
            },
            net::SocketAddr::V6(v6) => Self {
                storage: InetStorage {
                    v6: <backend::Backend as backend::Socket>::encode_v6(v6),
                },
                len: size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            },
        }
    }

    pub fn mut_ptr(&mut self) -> *mut libc::sockaddr {
        &raw mut self.storage as *mut libc::sockaddr
    }

    pub fn ptr(&self) -> *const libc::sockaddr {
        &raw const self.storage as *const libc::sockaddr
    }

    pub fn socklen(&self) -> libc::socklen_t {
        self.len
    }
}
