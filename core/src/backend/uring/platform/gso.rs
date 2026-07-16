use crate::io::socket::msg::MsgHdr;
use std::ptr;

pub const MAX_GSO_SEGMENTS: usize = 64;
pub const MAX_GSO_BYTES: usize = 65535;

#[derive(Default)]
#[repr(C, align(8))]
pub struct Gso([u8; 32]);

impl Gso {
    pub const fn new() -> Self {
        Self([0; 32])
    }

    pub fn attach(&mut self, msg: &mut MsgHdr, segment_size: u16) {
        if segment_size == 0 {
            return;
        }
        const DATA_LEN: u32 = size_of::<u16>() as u32;
        let (buf, cap) = (self.0.as_mut_ptr(), self.0.len());
        unsafe {
            let controllen = libc::CMSG_SPACE(DATA_LEN) as usize;
            debug_assert!(controllen <= cap);
            msg.set_control(buf.cast(), controllen);
            let hdr = libc::CMSG_FIRSTHDR(msg.raw());
            (*hdr).cmsg_level = libc::SOL_UDP;
            (*hdr).cmsg_type = libc::UDP_SEGMENT;
            (*hdr).cmsg_len = libc::CMSG_LEN(DATA_LEN) as _;
            ptr::copy_nonoverlapping(
                ptr::addr_of!(segment_size).cast::<u8>(),
                libc::CMSG_DATA(hdr),
                DATA_LEN as usize,
            );
        }
    }
}
