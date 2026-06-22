use super::Driver;
use crate::backend::Datagram;
use crate::backend::datagram::Outcome;
use crate::backend::lend::BidGuard;
use crate::backend::socket::Addr;

impl Datagram for Driver {
    fn recv_packet<'a>(
        &'a mut self,
        len: u32,
        bid: u16,
        msghdr: &libc::msghdr,
    ) -> (Outcome<'a>, BidGuard<'a, Self>) {
        let guard = BidGuard::new(self, len, bid);
        let raw = guard.slice();
        let namelen = msghdr.msg_namelen as usize;
        if raw.len() < namelen {
            return (Outcome::Error(0), guard);
        }
        let (name, payload) = raw.split_at(namelen);
        let Some(src) = Addr::parse_msg_name(name) else {
            return (Outcome::Error(0), guard);
        };
        let outcome = if payload.is_empty() {
            Outcome::Empty
        } else {
            Outcome::Packet { src, payload }
        };
        (outcome, guard)
    }
}
