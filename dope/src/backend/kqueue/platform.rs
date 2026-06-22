pub use crate::backend::pipe::Pipe;

use crate::backend::sockaddr::Stamp;

pub use super::system::{Mismatch, Snapshot};

impl Stamp for libc::sockaddr_in {
    fn stamp(&mut self) {
        self.sin_len = size_of::<libc::sockaddr_in>() as u8;
    }
}
impl Stamp for libc::sockaddr_in6 {
    fn stamp(&mut self) {
        self.sin6_len = size_of::<libc::sockaddr_in6>() as u8;
    }
}
impl Stamp for libc::sockaddr_un {
    fn stamp(&mut self) {
        self.sun_len = size_of::<libc::sockaddr_un>() as u8;
    }
}
