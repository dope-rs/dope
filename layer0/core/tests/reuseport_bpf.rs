use std::{mem, net, os::fd};

use dope_core::io::datagram;
use dope_sys::io::socket::route::{
    Plan, count,
    selector::{Bootstrap, PrefixPlan},
};
use dope_test::alloc::Tracker;

pub mod common;
use common::rt::route::{self, Packet, SELECTOR};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn selector_routes_and_rejects_packets() {
    let count = count::Count::try_from(2).expect("route count");
    let prefix = PrefixPlan::fixed::<31, 0xc0, 0x14, 0x16>();
    for framing in [None, Some(prefix)] {
        let mut plan = Plan::new(count, SELECTOR);
        if let Some(prefix) = framing {
            plan = plan.with_prefix(prefix);
        }
        let address = net::SocketAddr::from(([127, 0, 0, 1], 0));
        let group = datagram::Group::bind(address, plan);
        if !route::is_available(&group, address, plan) {
            return;
        }
        let mut group = group.expect("routing group");
        let mut packets = Vec::new();
        for key in 0..2 {
            for (first, length, key_at) in [(0x40, 10, 1), (0x80, 16, 6)] {
                let mut packet = vec![0; length];
                packet[0] = first;
                packet[5] = 9;
                packet[key_at] = key as u8;
                packets.push(Packet {
                    bytes: packet.clone(),
                    worker: Some(key),
                    broadcast: false,
                });
                if let Some(prefix) = framing {
                    for tag in prefix.tags() {
                        let mut bytes = vec![0; 31];
                        bytes[0] = tag;
                        bytes.extend_from_slice(&packet);
                        packets.push(Packet {
                            bytes,
                            worker: Some(key),
                            broadcast: false,
                        });
                    }
                }
            }
        }
        packets.push(Packet {
            bytes: vec![0x80; 15],
            worker: None,
            broadcast: false,
        });
        if framing.is_some() {
            for (inner, length, declared, key) in [
                (0x40, 31, 9, 0),
                (0x14, 47, 9, 0),
                (0x16, 47, 9, 0),
                (0x15, 47, 9, 0),
                (0x80, 46, 9, 0),
                (0x80, 47, 8, 0),
                (0x40, 41, 9, 2),
            ] {
                let mut bytes = vec![0; 47];
                bytes[0] = 0x14;
                bytes[31] = inner;
                bytes[32] = key;
                bytes[36] = declared;
                bytes[37] = key;
                bytes.truncate(length);
                packets.push(Packet {
                    bytes,
                    worker: None,
                    broadcast: false,
                });
            }
            let mut bytes = vec![0; 47];
            bytes[0] = 0x15;
            bytes[31] = 0x40;
            packets.push(Packet {
                bytes,
                worker: None,
                broadcast: false,
            });
        }
        route::run(&mut group, packets, false);
    }
}

#[test]
fn bootstrap_routes_variable_keys() {
    let bootstrap = Bootstrap::fixed::<1, 1, 0xe0, 0xc0, 8, 20>();
    let prefix = PrefixPlan::fixed::<31, 0xc0, 0x14, 0x16>();
    let cid = [0xa5; 255];
    let issued = [1; 9];
    let mut tail = [0xa5; 20];
    tail[19] = 0xa4;
    for (width, workers, bootstrap) in [
        (2_u16, [Some(1), Some(1), Some(1), Some(0)], bootstrap),
        (3, [Some(1), Some(1), Some(2), Some(0)], bootstrap),
        (
            2,
            [Some(1), None, None, None],
            Bootstrap::fixed::<1, 1, 0xe0, 0xc0, 8, 8>(),
        ),
    ] {
        let count = count::Count::try_from(width).expect("dense route count");
        let outside = [width as u8; 9];
        for framing in [None, Some(prefix)] {
            let mut plan = Plan::new(count, SELECTOR).with_bootstrap(bootstrap);
            if let Some(prefix) = framing {
                plan = plan.with_prefix(prefix);
            }
            let address = net::SocketAddr::from(([127, 0, 0, 1], 0));
            let group = datagram::Group::bind(address, plan);
            if !route::is_available(&group, address, plan) {
                return;
            }
            let mut group = group.expect("bootstrap group");
            let mut packets = Vec::new();
            for (first, version, key, expected, missing) in [
                (0x40, 0_u32, issued.as_slice(), Some(1), 0),
                (0x80, 0, issued.as_slice(), Some(1), 0),
                (0xc0, 1, issued.as_slice(), Some(1), 0),
                (0xd0, 1, issued.as_slice(), Some(1), 0),
                (0xe0, 1, issued.as_slice(), Some(1), 0),
                (0x80, 0x87654321, issued.as_slice(), Some(1), 0),
                (0xc0, 1, &cid[..8], workers[0], 0),
                (0xd0, 1, &cid[..8], workers[0], 0),
                (0xc0, 1, &cid[..9], workers[1], 0),
                (0xd0, 1, &cid[..9], workers[1], 0),
                (0xc0, 1, &cid[..20], workers[2], 0),
                (0xd0, 1, &cid[..20], workers[2], 0),
                (0xc0, 1, tail.as_slice(), workers[3], 0),
                (0x80, 0x87654321, &cid[..0], Some(0), 0),
                (0x90, 2, &cid[..7], Some(0), 0),
                (0xa0, 2, &cid[..21], Some(0), 0),
                (0xb0, 2, cid.as_slice(), Some(0), 0),
                (0x40, 0, outside.as_slice(), None, 0),
                (0x40, 0, issued.as_slice(), None, 1),
                (0xc0, 0, &cid[..8], None, 0),
                (0x80, 1, &cid[..8], None, 0),
                (0xe0, 1, &cid[..8], None, 0),
                (0xc0, 1, &cid[..7], None, 0),
                (0xc0, 1, &cid[..21], None, 0),
                (0xc0, 1, &cid[..20], None, 2),
                (0x80, 2, &cid[..0], None, 3),
            ] {
                let mut packet = [0; 293];
                let base = framing.map_or(0, |prefix| {
                    packet[0] = prefix.tags()[0];
                    usize::from(prefix.extent().get())
                });
                packet[base] = first;
                let start = if first & 0x80 != 0 {
                    packet[base + 1..base + 5].copy_from_slice(&version.to_be_bytes());
                    packet[base + 5] = key.len() as u8;
                    base + 6
                } else {
                    base + 1
                };
                packet[start..start + key.len()].copy_from_slice(key);
                let end = start + key.len() + usize::from(first & 0x80 != 0) - missing;
                packets.push(Packet {
                    bytes: packet[..end].to_vec(),
                    worker: expected,
                    broadcast: false,
                });
            }
            route::run(&mut group, packets, false);
        }
    }
}

#[test]
fn wildcard_filters_fanout_and_retains_retired_indices() {
    let count = count::Count::try_from(3).expect("route count");
    let plan = Plan::new(count, SELECTOR);
    for address in [
        net::SocketAddr::from(([0, 0, 0, 0], 0)),
        net::SocketAddr::from((net::Ipv6Addr::UNSPECIFIED, 0)),
    ] {
        let group = datagram::Group::bind(address, plan);
        if !route::is_available(&group, address, plan) {
            return;
        }
        let mut group = group.expect("wildcard group");
        let mut packets = Vec::new();
        for broadcast in [false, true]
            .into_iter()
            .take(if address.is_ipv4() { 2 } else { 1 })
        {
            for key in [1, 2, 3] {
                let mut bytes = vec![0; 10];
                bytes[1] = key;
                packets.push(Packet {
                    bytes,
                    worker: (key < 3).then_some(usize::from(key)),
                    broadcast,
                });
            }
        }
        route::run(&mut group, packets, true);
        let packets = [0, 2].map(|key| {
            let mut bytes = vec![0; 10];
            bytes[1] = key;
            Packet {
                bytes,
                worker: Some(usize::from(key)),
                broadcast: false,
            }
        });
        route::run(&mut group, packets.into(), false);
    }
}

#[test]
fn preserves_dense_routes() {
    for raw in [1, 3, 256] {
        let count = count::Count::try_from(raw).expect("route count");
        let address = net::SocketAddr::from(([127, 0, 0, 1], 0));
        let plan = Plan::new(count, SELECTOR);
        let (group, activity) = Tracker::measure(|| datagram::Group::bind(address, plan));
        if !route::is_available(&group, address, plan) {
            return;
        }
        let mut group = group.expect("routing group");
        let bytes = usize::from(raw) * mem::size_of::<fd::OwnedFd>();
        assert_eq!(activity, [1, bytes, 0, 0]);
        assert_ne!(group.local_addr().port(), 0);
        let ((), activity) = Tracker::measure(|| {
            let mut entries = group.entries();
            assert_eq!(entries.len(), usize::from(raw));
            for ordinal in 0..usize::from(raw) {
                let (index, _lease) = entries.next().expect("admitted route");
                assert_eq!(index.into_raw(), ordinal);
                assert_eq!(entries.len(), usize::from(raw) - ordinal - 1);
            }
            assert!(entries.next().is_none());
        });
        assert_eq!(activity, [0; 4]);
        let ((), activity) = Tracker::measure(|| drop(group));
        assert_eq!(activity, [0, 0, 1, bytes]);
    }
}

#[test]
fn rejects_bound_address() {
    let occupied = net::UdpSocket::bind("127.0.0.1:0").expect("occupied address");
    let address = occupied.local_addr().expect("bound address");
    let count = count::Count::try_from(256).expect("route count");
    let plan = Plan::new(count, SELECTOR);
    let (failed, activity) = Tracker::measure(|| datagram::Group::bind(address, plan));
    if !route::is_available(&failed, address, plan) {
        return;
    }
    let error = failed.err().expect("occupied address admitted");
    assert!(
        matches!(error, datagram::Error::Syscall {
        operation: datagram::Operation::Bind, errno,
    } if errno.code().get() == libc::EADDRINUSE),
        "{error:?}"
    );
    assert_eq!(activity, [0; 4]);

    let address = net::SocketAddr::from(([0, 0, 0, 0], 0));
    let group = datagram::Group::bind(address, plan).expect("exclusive routed group");
    let error = datagram::Group::bind(group.local_addr(), plan)
        .err()
        .expect("independent group rejected");
    assert!(
        matches!(error, datagram::Error::Syscall {
        operation: datagram::Operation::Bind, errno,
    } if errno.code().get() == libc::EADDRINUSE),
        "{error:?}"
    );
}
