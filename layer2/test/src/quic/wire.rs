/// Maximum destination connection ID length in the QUIC wire format.
pub const MAX_CONNECTION_ID_BYTES: usize = 20;

pub struct ShortHeader<'packet> {
    packet: &'packet [u8],
}

impl<'packet> ShortHeader<'packet> {
    pub const fn new(packet: &'packet [u8]) -> Self {
        Self { packet }
    }

    /// Returns the destination CID using the deployment's public CID length.
    pub fn cid(self, cid_len: usize) -> Option<&'packet [u8]> {
        if cid_len == 0 || cid_len > MAX_CONNECTION_ID_BYTES {
            return None;
        }
        let end = 1usize.checked_add(cid_len)?;
        (self.packet.len() >= end && self.packet[0] & 0xc0 == 0x40).then(|| &self.packet[1..end])
    }
}
