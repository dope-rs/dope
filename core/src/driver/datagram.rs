use crate::backend::Backend;
use crate::backend::ops::datagram::DatagramBackend;
use crate::io::datagram::RecvOutcome;
use crate::io::provided::ProvidedLease;

use super::DriverRef;
use libc::msghdr;

pub trait Datagram {
    fn recv_packet(&self, buffer: &ProvidedLease<'_>, msghdr: &msghdr) -> RecvOutcome;
}

impl Datagram for DriverRef<'_> {
    fn recv_packet(&self, buffer: &ProvidedLease<'_>, msghdr: &msghdr) -> RecvOutcome {
        <Backend as DatagramBackend>::recv_packet(buffer, msghdr)
    }
}
