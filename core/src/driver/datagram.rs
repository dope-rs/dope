use crate::backend::Backend;
use crate::backend::ops::datagram::DatagramBackend;
use crate::io::datagram::RecvOutcome;
use crate::io::provided::ProvidedLease;

use super::DriverRef;

pub trait Datagram {
    fn recv_packet(&self, buffer: &ProvidedLease<'_>, msghdr: &libc::msghdr) -> RecvOutcome;
}

impl Datagram for DriverRef<'_> {
    fn recv_packet(&self, buffer: &ProvidedLease<'_>, msghdr: &libc::msghdr) -> RecvOutcome {
        <Backend as DatagramBackend>::recv_packet(buffer, msghdr)
    }
}
