use std::{mem, num};

use crate::{
    io::{datagram, socket::msg},
    platform,
};

#[repr(C, align(8))]
pub(crate) struct Control {
    buffer: Buffer,
    segment_size: num::NonZeroU16,
}

pub(crate) struct Capability;

impl platform::GsoMode for Control {
    type Capability = super::Capability;
    type Control = Self;

    const LIMITS: Option<datagram::GsoLimits> = Some(super::LIMITS);

    fn acquire() -> Option<Self::Capability> {
        Some(Capability)
    }

    fn limits(_: &Self::Capability) -> datagram::GsoLimits {
        super::LIMITS
    }

    fn control(_: Self::Capability, segment_size: num::NonZeroU16) -> Self::Control {
        Self {
            buffer: Buffer::new(),
            segment_size,
        }
    }

    fn release(control: Self::Control) -> (Self::Capability, num::NonZeroU16) {
        (Capability, control.segment_size)
    }

    fn attach<'a>(control: &'a mut Self::Control, msg: &mut msg::Builder<'a>) {
        let segment_size = control.segment_size.get();
        control.buffer.attach(msg, segment_size);
    }
}

struct Buffer([u8; 32]);

impl Buffer {
    const fn new() -> Self {
        Self([0; 32])
    }

    fn attach<'a>(&'a mut self, msg: &mut msg::Builder<'a>, segment_size: u16) {
        const DATA_LEN: u32 = mem::size_of::<u16>() as u32;
        let (buf, cap) = (self.0.as_mut_ptr(), self.0.len());
        unsafe {
            use std::ptr::{addr_of, copy_nonoverlapping};

            use libc::{CMSG_DATA, CMSG_LEN, CMSG_SPACE, SOL_UDP, UDP_SEGMENT};
            let controllen = CMSG_SPACE(DATA_LEN) as usize;
            debug_assert!(controllen <= cap);
            let hdr = buf.cast::<libc::cmsghdr>();
            (*hdr).cmsg_level = SOL_UDP;
            (*hdr).cmsg_type = UDP_SEGMENT;
            (*hdr).cmsg_len = CMSG_LEN(DATA_LEN) as _;
            copy_nonoverlapping(
                addr_of!(segment_size).cast::<u8>(),
                CMSG_DATA(hdr),
                DATA_LEN as usize,
            );
            msg.control(&self.0[..controllen]);
        }
    }
}
