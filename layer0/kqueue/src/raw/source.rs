use std::{convert::Infallible, mem, net, slice};

use dope_sys::io::socket::msg::source;

use crate::Kqueue;

/// One initialized Darwin packet-info control message, prepared once per source.
#[derive(Clone, Copy, Debug)]
#[repr(align(4))]
pub struct Control([u8; 32]);

impl source::Source for Kqueue {
    type Control = Control;
    type Combined = Control;

    fn control(source: source::Address) -> Control {
        let mut control = Control([0; 32]);
        let source = source.get();
        let scope = match source {
            net::SocketAddr::V4(_) => 0,
            net::SocketAddr::V6(source) => source.scope_id(),
        };
        let (level, kind, len) = match source.ip().to_canonical() {
            net::IpAddr::V4(source) => {
                control.0[16..20].copy_from_slice(&source.octets());
                (
                    libc::IPPROTO_IP,
                    libc::IP_PKTINFO,
                    mem::size_of::<libc::in_pktinfo>(),
                )
            }
            net::IpAddr::V6(source) => {
                control.0[12..28].copy_from_slice(&source.octets());
                control.0[28..32].copy_from_slice(&scope.to_ne_bytes());
                (
                    libc::IPPROTO_IPV6,
                    libc::IPV6_PKTINFO,
                    mem::size_of::<libc::in6_pktinfo>(),
                )
            }
        };
        // SAFETY: both payload sizes fit c_uint and the native control message.
        let len = unsafe { libc::CMSG_LEN(len as libc::c_uint) };
        control.0[..4].copy_from_slice(&len.to_ne_bytes());
        control.0[4..8].copy_from_slice(&level.to_ne_bytes());
        control.0[8..12].copy_from_slice(&kind.to_ne_bytes());
        control
    }

    fn compose(source: Control, _: Option<Infallible>) -> Control {
        source
    }

    fn release(control: Control) -> (Control, Option<Infallible>) {
        (control, None)
    }

    fn bytes(control: &mut Control) -> &[u8] {
        Control::as_ref(control)
    }
}

impl AsRef<[u8]> for Control {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: construction initializes every header/payload byte; the layout
        // assertions exclude padding and cmsg_len retains the selected payload bound.
        let len = u32::from_ne_bytes([self.0[0], self.0[1], self.0[2], self.0[3]]);
        unsafe { slice::from_raw_parts(self.0.as_ptr(), len as usize) }
    }
}

const _: () = {
    assert!(mem::size_of::<libc::cmsghdr>() == 12);
    assert!(mem::offset_of!(libc::cmsghdr, cmsg_len) == 0);
    assert!(mem::offset_of!(libc::cmsghdr, cmsg_level) == 4);
    assert!(mem::offset_of!(libc::cmsghdr, cmsg_type) == 8);
    assert!(mem::align_of::<libc::cmsghdr>() == mem::align_of::<Control>());
    assert!(mem::size_of::<Control>() == 32);
    assert!(mem::size_of::<libc::in_pktinfo>() == 12);
    assert!(mem::offset_of!(libc::in_pktinfo, ipi_spec_dst) == 4);
    assert!(mem::size_of::<libc::in6_pktinfo>() == 20);
    assert!(mem::offset_of!(libc::in6_pktinfo, ipi6_addr) == 0);
    assert!(mem::offset_of!(libc::in6_pktinfo, ipi6_ifindex) == 16);
};
