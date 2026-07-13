use super::Driver;
use crate::backend::Datagram;
use crate::backend::lend::BidGuard;
use crate::datagram::Outcome;
use crate::socket;

impl Datagram for Driver {
    fn recv_packet<'a>(
        &'a self,
        len: u32,
        bid: u16,
        msghdr: &libc::msghdr,
    ) -> (Outcome<'a>, BidGuard<'a, Self>) {
        let guard = BidGuard::new(self, len, bid);
        let raw = guard.slice();
        let Ok(parsed) = io_uring::types::RecvMsgOut::parse(raw, msghdr) else {
            return (Outcome::Error(0), guard);
        };
        let truncated = parsed.is_payload_truncated() || parsed.is_name_data_truncated();
        let Some(src) = socket::Addr::parse_msg_name(parsed.name_data()) else {
            return (Outcome::Error(0), guard);
        };
        let payload = parsed.payload_data();
        let off = payload.as_ptr() as usize - raw.as_ptr() as usize;
        let payload = &raw[off..off + payload.len()];
        let outcome = if truncated {
            Outcome::Truncated { src, partial: payload }
        } else if payload.is_empty() {
            Outcome::Empty
        } else {
            Outcome::Packet { src, payload }
        };
        (outcome, guard)
    }
}
