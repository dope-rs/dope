use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    path::Path,
};

use dope_core::io::socket;

#[test]
fn internet_addresses_round_trip_without_losing_identity() {
    let addresses = [
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 7), 5353)),
        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 5353, 17, 4)),
    ];
    for address in addresses {
        assert_eq!(socket::Addr::from_std(address).into_std().unwrap(), address);
    }
}

#[test]
fn stream_specs_are_derived_from_every_supported_address_family() {
    let internet = [
        socket::Addr::from_std(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9))),
        socket::Addr::from_std(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::LOCALHOST,
            9,
            0,
            0,
        ))),
    ];
    for peer in &internet {
        socket::StreamSpec::for_peer(peer).expect("internet stream spec");
    }

    let unix =
        socket::Addr::from_unix_path(Path::new("/tmp/dope-stream-spec")).expect("unix address");
    socket::StreamSpec::for_peer(&unix).expect("unix stream spec");
}
