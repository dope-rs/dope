use super::DriverRef;
use crate::io::datagram::RecvOutcome;
use crate::io::provided::ProvidedLease;
use crate::io::socket::addr::Addr;

pub trait Datagram {
    fn recv_packet(&self, buffer: &ProvidedLease<'_>, msghdr: &libc::msghdr) -> RecvOutcome;
}

cfg_select! {
    target_os = "linux" => {
        use io_uring::types::RecvMsgOut;

        impl Datagram for DriverRef<'_> {
            fn recv_packet(&self, buffer: &ProvidedLease<'_>, msghdr: &libc::msghdr) -> RecvOutcome {
                let raw = buffer.as_slice();
                let Ok(parsed) = RecvMsgOut::parse(raw, msghdr) else {
                    return RecvOutcome::Error(0);
                };
                let truncated = parsed.is_payload_truncated() || parsed.is_name_data_truncated();
                let Some(src) = Addr::parse_msg_name(parsed.name_data()) else {
                    return RecvOutcome::Error(0);
                };
                let payload = parsed.payload_data();
                let off = payload.as_ptr() as usize - raw.as_ptr() as usize;
                let payload = off..off + payload.len();
                if truncated {
                    RecvOutcome::Truncated {
                        src,
                        partial: payload,
                    }
                } else if payload.is_empty() {
                    RecvOutcome::Empty
                } else {
                    RecvOutcome::Packet { src, payload }
                }
            }
        }
    }
    _ => {
        impl Datagram for DriverRef<'_> {
            fn recv_packet(&self, buffer: &ProvidedLease<'_>, msghdr: &libc::msghdr) -> RecvOutcome {
                let raw = buffer.as_slice();
                let namelen = msghdr.msg_namelen as usize;
                if raw.len() < namelen {
                    return RecvOutcome::Error(0);
                }
                let (name, payload) = raw.split_at(namelen);
                let Some(src) = Addr::parse_msg_name(name) else {
                    return RecvOutcome::Error(0);
                };
                if payload.is_empty() {
                    RecvOutcome::Empty
                } else {
                    RecvOutcome::Packet {
                        src,
                        payload: namelen..raw.len(),
                    }
                }
            }
        }
    }
}
