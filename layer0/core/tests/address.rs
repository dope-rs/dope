use std::{
    ffi::OsStr,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::unix::ffi::OsStrExt as _,
    path::Path,
};

use dope_core::io::socket;
use socket::Prepared as _;

#[test]
fn internet_round_trip() {
    let addrs = [
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 7), 5353)),
        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 5353, 17, 4)),
    ];
    for addr in addrs {
        assert_eq!(socket::Addr::from_std(addr).into_std(), Ok(addr));
    }
}

#[test]
fn stream_families() {
    let v4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9);
    let v6 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 9, 0, 0);
    let v4 = socket::Inet::from_std(v4.into()).stream_spec();
    let v6 = socket::Inet::from_std(v6.into()).stream_spec();
    assert!(v4 != v6);
    let unix = (socket::Unix::from_path(Path::new("/tmp/dope-stream-spec")))
        .expect("bounded Unix address");
    assert!(unix.stream_spec() != v4);
    assert!(unix.stream_spec() != v6);
}

#[test]
fn path_errors() {
    assert!(matches!(
        socket::Unix::from_path(Path::new("")),
        Err(socket::Error::PathEmpty)
    ));

    let long = vec![b'a'; 4_096];
    assert!(matches!(
        socket::Unix::from_path(Path::new(OsStr::from_bytes(&long))),
        Err(socket::Error::PathLength { actual, maximum })
            if actual == long.len() && maximum < actual
    ));

    let bytes = b"/tmp/dope\0suffix";
    assert!(matches!(
        socket::Unix::from_path(Path::new(OsStr::from_bytes(bytes))),
        Err(socket::Error::PathNul { offset: 9 })
    ));
}
