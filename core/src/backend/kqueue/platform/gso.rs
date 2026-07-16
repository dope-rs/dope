use crate::io::socket::msg::MsgHdr;

pub const MAX_GSO_SEGMENTS: usize = 1;
pub const MAX_GSO_BYTES: usize = usize::MAX;

#[derive(Default)]
pub struct Gso;

impl Gso {
    pub const fn new() -> Self {
        Self
    }

    pub fn attach(&mut self, msg: &mut MsgHdr, segment_size: u16) {
        let _ = msg;
        debug_assert_eq!(segment_size, 0);
    }
}
