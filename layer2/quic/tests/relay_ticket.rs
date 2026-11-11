use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use dope_quic::security::relay::{Key, Reject, Ticket};

#[test]
fn original_address_is_lossless() {
    let key = Key::try_new().expect("worker entropy");
    let v4 = Ipv4Addr::new(192, 0, 2, 8);
    let v6 = "fe80::1234"
        .parse::<Ipv6Addr>()
        .expect("link local address");
    for original in [
        SocketAddr::V4(SocketAddrV4::new(v4, 12_345)),
        SocketAddr::V6(SocketAddrV6::new(v6, 54_321, 0x1234, 7)),
        SocketAddr::V6(SocketAddrV6::new(v4.to_ipv6_mapped(), 12_345, 9, 11)),
    ] {
        let ticket = key.issue(u32::MAX, original);
        assert_eq!(ticket.as_bytes().len(), 31);
        assert_eq!(ticket.source_index(), u32::MAX);
        let parsed = Ticket::try_from(ticket.as_bytes()).expect("issued format");
        assert_eq!(parsed, ticket);
        assert_eq!(
            key.verify(&parsed, original.ip().to_canonical()),
            Ok(original)
        );
    }
}

#[test]
fn altered_route_and_other_worker_are_rejected() {
    let key = Key::try_new().expect("worker entropy");
    let other = Key::try_new().expect("independent worker entropy");
    let address = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 443, 7, 3));
    let ticket = key.issue(5, address);
    for offset in 1..Ticket::LEN {
        let mut bytes = *ticket.as_bytes();
        bytes[offset] ^= 1;
        let altered = Ticket::try_from(&bytes).expect("altered fixed fields remain well formed");
        assert_eq!(
            key.verify(&altered, address.ip()),
            Err(Reject::Authentication)
        );
    }
    assert_eq!(
        other.verify(&ticket, address.ip()),
        Err(Reject::Authentication)
    );
    let changed = "2001:db8::1".parse::<IpAddr>().expect("changed source");
    assert_eq!(key.verify(&ticket, changed), Err(Reject::Authentication));

    let v4 = SocketAddr::from(([192, 0, 2, 8], 443));
    let mut bytes = *key.issue(5, v4).as_bytes();
    bytes[0] = 0x16;
    let mapped = Ticket::try_from(&bytes).expect("well formed mapped IPv6 substitution");
    assert_eq!(key.verify(&mapped, v4.ip()), Err(Reject::Authentication));
}

#[test]
fn malformed_tickets_are_classified_before_authentication() {
    assert_eq!(Ticket::try_from(&[][..]), Err(Reject::Length { actual: 0 }));
    assert_eq!(
        Ticket::try_from(&[0; 32][..]),
        Err(Reject::Length { actual: 32 })
    );
    let mut bytes = [0; Ticket::LEN];
    assert_eq!(Ticket::try_from(&bytes), Err(Reject::Marker(0)));
    bytes[0] = 0x14;
    bytes[10] = 7;
    bytes[14] = 3;
    assert_eq!(
        Ticket::try_from(&bytes),
        Err(Reject::Ipv4 {
            flowinfo: 7,
            scope_id: 3
        })
    );
    bytes[7..15].fill(0);
    let ticket = Ticket::try_from(&bytes).expect("well formed IPv4 ticket");
    let key = Key::try_new().expect("worker entropy");
    let address = IpAddr::V6(Ipv6Addr::LOCALHOST);
    assert_eq!(
        key.verify(&ticket, address),
        Err(Reject::Source {
            marker: 0x14,
            address
        })
    );
}
