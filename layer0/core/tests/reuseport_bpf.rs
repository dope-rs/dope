use std::{io, mem, net, os::fd, time};

use dope_core::io::datagram;
use dope_sys::io::socket::route::{
    Plan, count,
    selector::{Bootstrap, PrefixPlan},
};

pub mod common;
use common::rt::route::{self, SELECTOR};
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn selector_routes_packets() {
    let count = { (count::Count::try_from(2)).expect("count rejected: {error:?}") };
    let selector = SELECTOR;
    let addr = { ("127.0.0.1:0".parse()).expect("loopback address rejected: {error}") };
    let plan = Plan::new(count, selector);
    let group = datagram::Group::bind(addr, plan);
    if !route::is_available(&group, addr, plan) {
        return;
    }
    let group = group.expect("required fixture outcome");
    let mut entries = group.into_entries();
    assert_eq!(entries.len(), count.indices().len());
    let zero = {
        let (index, lease) = (entries.next()).expect("route zero missing");
        {
            assert_eq!(index.into_raw(), 0);
            net::UdpSocket::from(lease.into_fd())
        }
    };
    let addr = zero.local_addr().expect("route zero address unavailable");
    assert_ne!(addr.port(), 0);
    let one = {
        let (index, lease) = (entries.next()).expect("route one missing");
        {
            assert_eq!(index.into_raw(), 1);
            net::UdpSocket::from(lease.into_fd())
        }
    };
    assert!(entries.next().is_none());
    let timeout = Some(time::Duration::from_millis(100));
    {
        (zero.set_read_timeout(timeout)).expect("route zero timeout rejected: {error}");
    }
    {
        (one.set_read_timeout(timeout)).expect("route one timeout rejected: {error}");
    }
    let sender = { (net::UdpSocket::bind("127.0.0.1:0")).expect("sender unavailable: {error}") };

    for (key, receiver) in [(0, &zero), (1, &one)] {
        let mut clear = [0_u8; 10];
        clear[1] = key;
        {
            (sender.send_to(&clear, addr)).expect("send clear failed: {error}");
        }
        let mut received = [0_u8; 32];
        let len = {
            let (len, _) =
                (receiver.recv_from(&mut received)).expect("receive clear failed: {error}");
            len
        };
        assert_eq!(&received[..len], &clear);

        let mut set = [0_u8; 16];
        set[0] = 0x80;
        set[5] = 9;
        set[6] = key;
        {
            (sender.send_to(&set, addr)).expect("send set failed: {error}");
        }
        let len = {
            let (len, _) =
                (receiver.recv_from(&mut received)).expect("receive set failed: {error}");
            len
        };
        assert_eq!(&received[..len], &set);
    }

    let invalid = [0x80_u8; 15];
    {
        (sender.send_to(&invalid, addr)).expect("send invalid failed: {error}");
    }
    for receiver in [&zero, &one] {
        let mut received = [0_u8; 32];
        {
            let result = receiver.recv_from(&mut received);
            assert!(
                matches!(result, Err(error) if matches!(
                    error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                )),
                "selector delivered invalid packet"
            );
        }
    }
}

#[test]
fn dual_framing_routes_packets() {
    let count = count::Count::try_from(2).expect("valid route count");
    let prefix = PrefixPlan::fixed::<31, 0xc0, 0x14, 0x16>();
    let plan = Plan::new(count, SELECTOR).with_prefix(prefix);
    let address = net::SocketAddr::from(([127, 0, 0, 1], 0));
    let group = datagram::Group::bind(address, plan);
    if !route::is_available(&group, address, plan) {
        return;
    }
    let mut entries = group.expect("dual selector rejected").into_entries();
    let receivers: [net::UdpSocket; 2] = std::array::from_fn(|_| {
        net::UdpSocket::from(entries.next().expect("admitted route missing").1.into_fd())
    });
    let address = receivers[0]
        .local_addr()
        .expect("route address unavailable");
    let sender = net::UdpSocket::bind("127.0.0.1:0").expect("sender unavailable");
    for (key, receiver) in receivers.iter().enumerate() {
        receiver
            .set_read_timeout(Some(time::Duration::from_millis(100)))
            .expect("route timeout rejected");
        // A long header with its fixed bit clear includes version negotiation.
        for (first, length, key_at) in [(0x40, 10, 1), (0x80, 16, 6)] {
            let mut packet = [0; 47];
            packet[31] = first;
            packet[36] = 9;
            packet[31 + key_at] = key as u8;
            let mut received = [0; 47];
            sender
                .send_to(&packet[31..31 + length], address)
                .expect("send direct failed");
            let (len, _) = receiver
                .recv_from(&mut received)
                .expect("receive direct failed");
            assert_eq!(&received[..len], &packet[31..31 + length]);
            for tag in prefix.tags() {
                packet[0] = tag;
                sender
                    .send_to(&packet[..31 + length], address)
                    .expect("send prefixed failed");
                let (len, _) = receiver
                    .recv_from(&mut received)
                    .expect("receive prefixed failed");
                assert_eq!(&received[..len], &packet[..31 + length]);
            }
        }
    }
    let mut invalid = [0; 47];
    invalid[0] = 0x14;
    for (inner, length, declared, key) in [
        (0x40, 31, 9, 0),
        (0x14, 47, 9, 0),
        (0x16, 47, 9, 0),
        (0x15, 47, 9, 0),
        (0x80, 46, 9, 0),
        (0x80, 47, 8, 0),
        (0x40, 41, 9, 2),
    ] {
        invalid[31] = inner;
        invalid[32] = key;
        invalid[36] = declared;
        invalid[37] = key;
        sender
            .send_to(&invalid[..length], address)
            .expect("send invalid failed");
    }
    // The explicit dual format rejects unknown tags in the non-QUIC domain.
    invalid[0] = 0x15;
    invalid[31] = 0x40;
    invalid[32] = 0;
    sender
        .send_to(&invalid, address)
        .expect("send unknown failed");
    for receiver in &receivers {
        let mut received = [0; 47];
        assert!(matches!(
            receiver.recv_from(&mut received),
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
        ));
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
    for (width, workers) in [(2_u16, [1, 1, 1, 0]), (3, [1, 1, 2, 0])] {
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
            let receivers: Vec<_> = group
                .expect("bootstrap group")
                .into_entries()
                .map(|(_, lease)| net::UdpSocket::from(lease.into_fd()))
                .collect();
            let address = receivers[0].local_addr().expect("bound address");
            for receiver in &receivers {
                receiver
                    .set_read_timeout(Some(time::Duration::from_millis(100)))
                    .expect("receive deadline");
            }
            let senders = [(); 2].map(|()| net::UdpSocket::bind("127.0.0.1:0").expect("sender"));
            assert_ne!(
                senders[0].local_addr().unwrap(),
                senders[1].local_addr().unwrap()
            );
            for (first, version, key, expected, missing) in [
                (0x40, 0_u32, issued.as_slice(), Some(1), 0),
                (0x80, 0, issued.as_slice(), Some(1), 0),
                (0xc0, 1, issued.as_slice(), Some(1), 0),
                (0xd0, 1, issued.as_slice(), Some(1), 0),
                (0xe0, 1, issued.as_slice(), Some(1), 0),
                (0x80, 0x87654321, issued.as_slice(), Some(1), 0),
                (0xc0, 1, &cid[..8], Some(workers[0]), 0),
                (0xd0, 1, &cid[..8], Some(workers[0]), 0),
                (0xc0, 1, &cid[..9], Some(workers[1]), 0),
                (0xd0, 1, &cid[..9], Some(workers[1]), 0),
                (0xc0, 1, &cid[..20], Some(workers[2]), 0),
                (0xd0, 1, &cid[..20], Some(workers[2]), 0),
                (0xc0, 1, tail.as_slice(), Some(workers[3]), 0),
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
                for sender in &senders {
                    sender
                        .send_to(&packet[..end], address)
                        .expect("send packet");
                    if let Some(index) = expected {
                        let mut received = [0; 293];
                        let (length, peer) = receivers[index]
                            .recv_from(&mut received)
                            .expect("selected worker");
                        assert_eq!(&received[..length], &packet[..end]);
                        assert_eq!(peer, sender.local_addr().expect("sender address"));
                    }
                }
            }
            for receiver in &receivers {
                assert!(matches!(
                    receiver.recv_from(&mut [0; 293]),
                    Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
                ));
            }
        }
    }
}

#[test]
fn preserves_dense_routes() {
    for raw in [1, 3, 256] {
        let count = (count::Count::try_from(raw)).expect("valid route count rejected");
        let address = net::SocketAddr::from(([127, 0, 0, 1], 0));
        let plan = Plan::new(count, SELECTOR);
        let (group, activity) = Tracker::measure(|| datagram::Group::bind(address, plan));
        if !route::is_available(&group, address, plan) {
            return;
        }
        let group = group.expect("required fixture outcome");
        let bytes = usize::from(raw) * mem::size_of::<fd::OwnedFd>();
        assert_eq!(activity, [1, bytes, 0, 0]);
        let ((), activity) = Tracker::measure(|| {
            let mut entries = group.into_entries();
            assert_eq!(entries.len(), usize::from(raw));
            let (index, lease) = entries.next().expect("route zero missing its socket");
            assert_eq!(index.into_raw(), 0);
            assert_eq!(entries.len(), usize::from(raw) - 1);
            let first = net::UdpSocket::from(lease.into_fd());
            let address = first.local_addr().expect("route zero address unavailable");
            assert_ne!(address.port(), 0);
            for ordinal in 1..usize::from(raw) {
                let (index, lease) = (entries.next()).expect("admitted route missing its socket");
                assert_eq!(index.into_raw(), ordinal);
                assert_eq!(entries.len(), usize::from(raw) - ordinal - 1);
                let socket = net::UdpSocket::from(lease.into_fd());
                assert_eq!(
                    socket.local_addr().expect("route address unavailable"),
                    address
                );
            }
            assert!(entries.next().is_none());
        });
        assert_eq!(activity, [0, 0, 1, bytes]);
    }
}

#[test]
fn rejects_bound_address() {
    let occupied = {
        (net::UdpSocket::bind("127.0.0.1:0")).expect("ephemeral UDP fixture unavailable: {error}")
    };
    let address = { (occupied.local_addr()).expect("fixture address unavailable: {error}") };
    let count = count::Count::try_from(256).expect("valid dense route count");
    let plan = Plan::new(count, SELECTOR);
    let (failed, activity) = Tracker::measure(|| datagram::Group::bind(address, plan));
    if !route::is_available(&failed, address, plan) {
        return;
    }
    let error = failed.err().expect("occupied address admitted");
    assert!(
        matches!(
            error,
            datagram::Error::Syscall {
                operation: datagram::Operation::Bind,
                errno,
            } if errno.code().get() == libc::EADDRINUSE
        ),
        "expected address-in-use bind error: {error:?}"
    );
    assert_eq!(activity, [0, 0, 0, 0]);
}
