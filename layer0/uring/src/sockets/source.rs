use std::{mem, net, ptr, slice};

use dope_sys::io::socket::msg::source;

use crate::{Uring, sockets::controls};

// SAFETY: CMSG_SPACE only aligns these fixed native packet-info payload sizes.
const V4: usize = unsafe { libc::CMSG_SPACE(mem::size_of::<libc::in_pktinfo>() as u32) as usize };
const V6: usize = unsafe { libc::CMSG_SPACE(mem::size_of::<libc::in6_pktinfo>() as u32) as usize };

/// Precomputed source selection retained through each datagram submission.
#[derive(Clone, Copy, Debug)]
#[repr(C, align(8))]
pub struct Control {
    bytes: [u8; V6],
}

/// One source with its optional retained segmentation capability.
#[repr(transparent)]
pub struct Combined(Kind);

enum Kind {
    Source(Control),
    Segmented(Segmented),
}

/// One contiguous native GSO message followed by its selected source message.
#[repr(C)]
struct Segmented {
    gso: controls::Control,
    source: Control,
}

impl source::Source for Uring {
    type Control = Control;
    type Combined = Combined;

    fn control(source: source::Address) -> Self::Control {
        let address = source.get();
        let mut control = Control { bytes: [0; V6] };
        let header = control.bytes.as_mut_ptr().cast::<libc::cmsghdr>();
        // SAFETY: Control owns aligned, zero-initialized storage for either
        // complete native message. Both packet-info layouts have no padding.
        unsafe {
            let payload = libc::CMSG_DATA(header);
            let length = match address {
                net::SocketAddr::V4(address) => {
                    (*header).cmsg_level = libc::IPPROTO_IP;
                    (*header).cmsg_type = libc::IP_PKTINFO;
                    payload.cast::<libc::in_pktinfo>().write(libc::in_pktinfo {
                        ipi_ifindex: 0,
                        ipi_spec_dst: libc::in_addr {
                            s_addr: u32::from_ne_bytes(address.ip().octets()),
                        },
                        ipi_addr: libc::in_addr { s_addr: 0 },
                    });
                    mem::size_of::<libc::in_pktinfo>()
                }
                net::SocketAddr::V6(address) => {
                    (*header).cmsg_level = libc::IPPROTO_IPV6;
                    (*header).cmsg_type = libc::IPV6_PKTINFO;
                    payload
                        .cast::<libc::in6_pktinfo>()
                        .write(libc::in6_pktinfo {
                            ipi6_addr: libc::in6_addr {
                                s6_addr: address.ip().octets(),
                            },
                            ipi6_ifindex: address.scope_id(),
                        });
                    mem::size_of::<libc::in6_pktinfo>()
                }
            };
            (*header).cmsg_len = libc::CMSG_LEN(length as u32) as _;
        }
        control
    }

    fn compose(source: Control, gso: Option<controls::Control>) -> Combined {
        Combined(match gso {
            Some(gso) => Kind::Segmented(Segmented { gso, source }),
            None => Kind::Source(source),
        })
    }

    fn release(control: Combined) -> (Control, Option<controls::Control>) {
        match control.0 {
            Kind::Source(source) => (source, None),
            Kind::Segmented(control) => (control.source, Some(control.gso)),
        }
    }

    fn bytes(control: &mut Combined) -> &[u8] {
        match &control.0 {
            Kind::Source(source) => source.as_ref(),
            Kind::Segmented(control) => {
                let length = mem::size_of::<controls::Control>() + control.source.as_ref().len();
                // SAFETY: this repr(C) payload, excluding the enclosing enum, keeps
                // the initialized GSO message and padding immediately before the
                // source message. Its final CMSG_LEN bounds this exact owner.
                unsafe { slice::from_raw_parts(ptr::from_ref(control).cast(), length) }
            }
        }
    }
}

impl AsRef<[u8]> for Control {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: the private constructor writes this aligned header's length
        // once, bounded by the initialized native message storage. A final
        // control message does not need trailing CMSG_SPACE padding.
        unsafe {
            let length = (*self.bytes.as_ptr().cast::<libc::cmsghdr>()).cmsg_len;
            slice::from_raw_parts(self.bytes.as_ptr(), length)
        }
    }
}

const _: () = {
    assert!(V4 <= V6);
    assert!(mem::size_of::<libc::in_pktinfo>() == 12);
    assert!(mem::size_of::<libc::in6_pktinfo>() == 20);
    assert!(mem::align_of::<Control>() >= mem::align_of::<libc::cmsghdr>());
    assert!(mem::align_of::<Control>() >= mem::align_of::<libc::in_pktinfo>());
    assert!(mem::align_of::<Control>() >= mem::align_of::<libc::in6_pktinfo>());
    assert!(mem::size_of::<Control>() == 40);
    assert!(mem::offset_of!(Segmented, gso) == 0);
    assert!(mem::offset_of!(Segmented, source) == mem::size_of::<controls::Control>());
    assert!(
        mem::size_of::<Segmented>()
            == mem::size_of::<controls::Control>() + mem::size_of::<Control>()
    );
    assert!(mem::align_of::<Segmented>() == mem::align_of::<Control>());
};
