use io_uring::types;

use crate::{
    backend::{
        self,
        uring::{self, capabilities::gso},
    },
    io::{datagram, recv, socket},
    platform,
};

impl platform::Datagram for backend::Uring {
    type Gso = gso::Control;

    fn project(buffer: &recv::Lease<'_>) -> datagram::Projection {
        use uring::ffi::recvmsg::header;

        let raw = buffer.as_slice();
        let Ok(parsed) = types::RecvMsgOut::parse(raw, header::Header::datagram()) else {
            return datagram::Projection::Rejected { truncated: false };
        };
        if parsed.is_payload_truncated() || parsed.is_name_data_truncated() {
            return datagram::Projection::Rejected { truncated: true };
        }
        let Some(source) = socket::Addr::parse_msg_name(parsed.name_data()) else {
            return datagram::Projection::Rejected { truncated: false };
        };
        let payload = buffer.span_of(parsed.payload_data());
        datagram::Projection::Packet { source, payload }
    }
}
