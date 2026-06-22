pub use super::system::{Mismatch, Snapshot};

pub use crate::backend::pipe::Pipe;

use crate::backend::sockaddr::Stamp;

impl Stamp for libc::sockaddr_in {
    fn stamp(&mut self) {}
}
impl Stamp for libc::sockaddr_in6 {
    fn stamp(&mut self) {}
}
impl Stamp for libc::sockaddr_un {
    fn stamp(&mut self) {}
}
