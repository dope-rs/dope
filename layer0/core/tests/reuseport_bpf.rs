use std::{io, mem, net, os::fd, time};

use dope_core::io::datagram;
use dope_sys::io::socket::route::{Plan, count};

pub mod common;
use common::rt::route::SELECTOR;
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

/// BPF map creation needs `CAP_BPF`; sandboxes without it answer `EPERM`.
fn is_bpf_denied(error: &datagram::Error) -> bool {
    let denied = matches!(
        error,
        datagram::Error::Syscall {
            operation: datagram::Operation::MapCreate,
            errno,
        } if errno.code().get() == libc::EPERM
    );
    if denied {
        eprintln!("skip: BPF map creation denied (EPERM)");
    }
    denied
}

#[test]
fn selector_routes_packets() {
    let count = { (count::Count::try_from(2)).expect("count rejected: {error:?}") };
    let selector = SELECTOR;
    let addr = { ("127.0.0.1:0".parse()).expect("loopback address rejected: {error}") };
    let group = datagram::Group::bind(addr, Plan::new(count, selector));
    if let Err(error) = &group
        && is_bpf_denied(error)
    {
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
fn preserves_dense_routes() {
    for raw in [1, 3, 256] {
        let count = (count::Count::try_from(raw)).expect("valid route count rejected");
        let address = net::SocketAddr::from(([127, 0, 0, 1], 0));
        let plan = Plan::new(count, SELECTOR);
        let (group, activity) = Tracker::measure(|| datagram::Group::bind(address, plan));
        if let Err(error) = &group
            && is_bpf_denied(error)
        {
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
    let error = failed.err().expect("occupied address admitted");
    if is_bpf_denied(&error) {
        return;
    }
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
