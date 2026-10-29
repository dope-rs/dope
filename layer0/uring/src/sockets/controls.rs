use std::{mem, num, ptr, slice};

use dope_sys::io::socket::gso;

const LENGTH: u32 = mem::size_of::<u16>() as u32;
// SAFETY: CMSG_SPACE only aligns the compile-time u16 payload length.
const SPACE: usize = unsafe { libc::CMSG_SPACE(LENGTH) as usize };
// SAFETY: CMSG_LEN only computes the native header extent for an empty payload.
const HEADER: usize = unsafe { libc::CMSG_LEN(0) as usize };

/// Native UDP segmentation authority.
pub struct Permit(());

/// Aligned ancillary storage whose payload retains its nonzero segment size.
#[repr(C, align(8))]
pub struct Control {
    header: libc::cmsghdr,
    size: num::NonZero<u16>,
    padding: [u8; SPACE - HEADER - LENGTH as usize],
}

impl gso::Mode for Control {
    type Capability = self::Permit;

    fn acquire() -> Option<Self::Capability> {
        Some(Permit(()))
    }

    fn control(cap: Self::Capability, size: num::NonZero<u16>) -> Self {
        let Permit(()) = cap;
        Self {
            header: libc::cmsghdr {
                cmsg_len: HEADER + LENGTH as usize,
                cmsg_level: libc::SOL_UDP,
                cmsg_type: libc::UDP_SEGMENT,
            },
            size,
            padding: [0; SPACE - HEADER - LENGTH as usize],
        }
    }

    fn release(self) -> (Self::Capability, num::NonZero<u16>) {
        (Permit(()), self.size)
    }

    fn bytes(&mut self) -> &[u8] {
        // SAFETY: the asserted ABI has no implicit padding: the native header,
        // nonzero payload and explicit trailing bytes are all initialized.
        // The shared byte view cannot alter the retained segment-size proof.
        unsafe { slice::from_raw_parts(ptr::from_ref(self).cast(), SPACE) }
    }
}

const _: () = {
    assert!(mem::size_of::<Permit>() == 0);
    assert!(mem::size_of::<libc::cmsghdr>() == HEADER);
    assert!(mem::offset_of!(libc::cmsghdr, cmsg_len) == 0);
    assert!(mem::offset_of!(libc::cmsghdr, cmsg_level) == mem::size_of::<usize>());
    assert!(mem::offset_of!(libc::cmsghdr, cmsg_type) == HEADER - mem::size_of::<libc::c_int>());
    assert!(HEADER == mem::size_of::<usize>() + 2 * mem::size_of::<libc::c_int>());
    assert!(mem::offset_of!(Control, size) == HEADER);
    assert!(mem::offset_of!(Control, padding) == HEADER + LENGTH as usize);
    assert!(mem::size_of::<Control>() == SPACE);
    assert!(mem::size_of::<Control>() == 24);
    assert!(mem::align_of::<Control>() == 8);
    assert!(mem::size_of::<Option<Control>>() == 24);
    assert!(mem::align_of::<Control>() >= mem::align_of::<libc::cmsghdr>());
};
