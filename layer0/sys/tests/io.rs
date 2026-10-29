use std::{
    cell::Cell,
    mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    ptr,
};

use addr::{Error, Internet, Storage};
use dope_sys::{Errno, io, io::socket::addr};

#[test]
fn empty_skips_errno() {
    let input = b"data";
    let called = Cell::new(false);
    let output = unsafe {
        io::raw::Buffer::try_native::<Errno>(&input[..], 0, || {
            called.set(true);
            Errno::BAD_DESCRIPTOR
        })
    }
    .expect("empty output is valid");
    assert!(!called.get(), "success must not read errno");
    assert!(output.is_empty());
    assert!(ptr::eq(output.as_ptr(), input.as_ptr()));
}

#[test]
fn failure_reads_errno_once() {
    let input = b"data";
    let calls = Cell::new(0);
    let errno = || {
        calls.set(calls.get() + 1);
        Errno::BAD_DESCRIPTOR
    };
    assert_eq!(
        unsafe { io::raw::Buffer::try_native::<Errno>(&input[..], -1, errno) },
        Err(Errno::BAD_DESCRIPTOR)
    );
    assert_eq!(calls.get(), 1);
}

#[test]
fn inet_raw() {
    for expected in [
        SocketAddr::from(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 7), 5353)),
        SocketAddr::from(SocketAddrV6::new(
            Ipv6Addr::new(0x2001, 0xdb8, 1, 2, 3, 4, 5, 6),
            9443,
            17,
            4,
        )),
    ] {
        let (size, align, port, ip, family) = match expected {
            SocketAddr::V4(_) => (
                mem::size_of::<libc::sockaddr_in>(),
                mem::align_of::<libc::sockaddr_in>(),
                mem::offset_of!(libc::sockaddr_in, sin_port),
                mem::offset_of!(libc::sockaddr_in, sin_addr),
                libc::AF_INET,
            ),
            SocketAddr::V6(_) => (
                mem::size_of::<libc::sockaddr_in6>(),
                mem::align_of::<libc::sockaddr_in6>(),
                mem::offset_of!(libc::sockaddr_in6, sin6_port),
                mem::offset_of!(libc::sockaddr_in6, sin6_addr),
                libc::AF_INET6,
            ),
        };
        let mut buffer = [0xa5; addr::CAPACITY + 1];
        let offset = usize::from((buffer.as_ptr() as usize).is_multiple_of(align));
        let bytes = &mut buffer[offset..offset + addr::CAPACITY];
        let start = mem::offset_of!(libc::sockaddr_storage, ss_family);
        bytes[start..addr::FAMILY_BYTES]
            .copy_from_slice(&(family as libc::sa_family_t).to_ne_bytes());
        bytes[port..port + 2].copy_from_slice(&expected.port().to_be_bytes());
        match expected {
            SocketAddr::V4(value) => {
                bytes[ip..ip + 4].copy_from_slice(&value.ip().octets());
            }
            SocketAddr::V6(value) => {
                bytes[ip..ip + 16].copy_from_slice(&value.ip().octets());
                let flow = mem::offset_of!(libc::sockaddr_in6, sin6_flowinfo);
                bytes[flow..flow + 4].copy_from_slice(&value.flowinfo().to_ne_bytes());
                let scope = mem::offset_of!(libc::sockaddr_in6, sin6_scope_id);
                bytes[scope..scope + 4].copy_from_slice(&value.scope_id().to_ne_bytes());
            }
        }
        assert_ne!(bytes.as_ptr() as usize % align, 0);
        for len in [size, addr::CAPACITY] {
            let bytes = &bytes[..len];
            assert_eq!(Internet::try_parse(bytes), Ok(expected));
            let mut storage = Storage::empty();
            let (raw, len) = storage.output_mut();
            // SAFETY: the initialized source fits this distinct native storage.
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), ptr::from_mut(raw).cast(), bytes.len())
            };
            *len = bytes.len() as libc::socklen_t;
            assert_eq!(storage.to_std(), Ok(expected));
        }
    }
}

#[test]
fn addr_err() {
    let header = addr::FAMILY_BYTES;
    let v4 = mem::size_of::<libc::sockaddr_in>();
    let v6 = mem::size_of::<libc::sockaddr_in6>();
    for (actual, family, error) in [
        (
            addr::CAPACITY + 1,
            libc::AF_UNSPEC,
            Error::Storage {
                actual: addr::CAPACITY + 1,
                capacity: addr::CAPACITY,
            },
        ),
        (
            0,
            libc::AF_INET,
            Error::Header {
                actual: 0,
                required: header,
            },
        ),
        (
            header - 1,
            libc::AF_INET,
            Error::Header {
                actual: header - 1,
                required: header,
            },
        ),
        (
            header,
            libc::AF_UNSPEC,
            Error::Unsupported {
                family: libc::AF_UNSPEC,
                actual: header,
            },
        ),
        (
            v4 - 1,
            libc::AF_INET,
            Error::Address {
                family: libc::AF_INET,
                actual: v4 - 1,
                required: v4,
            },
        ),
        (
            v6 - 1,
            libc::AF_INET6,
            Error::Address {
                family: libc::AF_INET6,
                actual: v6 - 1,
                required: v6,
            },
        ),
    ] {
        let family = family as libc::sa_family_t;
        let mut bytes = [0; addr::CAPACITY + 1];
        let start = mem::offset_of!(libc::sockaddr_storage, ss_family);
        bytes[start..header].copy_from_slice(&family.to_ne_bytes());
        assert_eq!(Internet::try_parse(&bytes[..actual]), Err(error));
        let mut storage = Storage::empty();
        let (raw, len) = storage.output_mut();
        raw.ss_family = family;
        *len = actual as libc::socklen_t;
        assert_eq!(storage.to_std(), Err(error));
    }
}
