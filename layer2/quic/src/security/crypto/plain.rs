use crate::wire::packet::header;

/// The adjacent header and body borrowed from one admitted packet.
pub(crate) struct Plain<'packet> {
    pub(super) head: &'packet [u8],
    pub(super) body: &'packet mut [u8],
    pub(super) packet_number: header::Number,
    pub(super) first: u8,
}
