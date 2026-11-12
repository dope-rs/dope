use crate::common::rt::{finish, recover};
use std::{
    io::Read as _,
    net::{TcpListener, TcpStream, UdpSocket},
    os::{fd::AsFd as _, unix::net::UnixStream},
    time::Duration,
};

use dope_core::{
    Backend, Errno,
    dope_sys::io::socket::Socket as _,
    driver::{life::recovery, route, settings},
    io::socket::{self, Listener, install, install::Install},
};
use dope_test::reclaim::Reports;

use crate::common::rt::Runtime;

pub mod common;

#[test]
fn adopts_known_listener() {
    let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener");
    let local = listener.local_addr();
    let runtime = Runtime::quic::<2, 8>();
    let mut reports = Reports::new::<route::Route>(runtime.config(), []);
    runtime.with_driver(
        |mut driver| {
            let (listener, address) =
                Install::adopt_bound(driver.reborrow(), listener).expect("install listener");
            assert_eq!(address, local);
            let _peer = TcpStream::connect(address).expect("installed listener");
            listener.close(&mut driver);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.events().expect("valid reclamation").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn connected_admission() {
    let listener = TcpListener::bind(common::LOCALHOST).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let stream = TcpStream::connect(address).expect("connected stream");
    let (mut peer, source) = listener.accept().expect("peer");
    stream.set_nonblocking(true).expect("nonblocking input");
    let control = stream.try_clone().expect("control descriptor");
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bounded close observation");
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<0, 1>())
        .run(|scope| {
            let mut driver = scope.context();
            let datagram = UdpSocket::bind(common::LOCALHOST).expect("wrong kind");
            assert!(matches!(
                Install::adopt_connected(driver.reborrow(), datagram.into()),
                Err(install::Error::Adopt(install::adopt::Error::StreamKind { actual }))
                    if actual == libc::SOCK_DGRAM
            ));
            assert!(matches!(
                Install::adopt_connected(driver.reborrow(), listener.into()),
                Err(install::Error::System { operation: install::Operation::Validate, fault })
                    if fault.is_not_connected()
            ));
            let (unix, _other) = UnixStream::pair().expect("non-Internet stream");
            assert!(matches!(
                Install::adopt_connected(driver.reborrow(), unix.into()),
                Err(install::Error::Address(socket::Error::Unsupported { family, .. }))
                    if family == libc::AF_UNIX
            ));
            let installed =
                Install::adopt_connected(driver.reborrow(), stream.into()).expect("returned slot");
            assert_eq!(control.local_addr().expect("preserved source"), source);
            assert_eq!(control.peer_addr().expect("preserved peer"), address);
            assert!(matches!(
                Backend::recv_now::<Errno>(control.as_fd(), &mut [0]),
                Err(error) if error.is_block()
            ));
            let datagram = UdpSocket::bind(common::LOCALHOST).expect("full input");
            assert!(matches!(
                Install::adopt_connected(driver.reborrow(), datagram.into()),
                Err(install::Error::Slot(install::slot::Error::Capacity {
                    available: 0
                }))
            ));
            drop(control);
            assert!(matches!(
                Backend::recv_now::<Errno>(peer.as_fd(), &mut [0]),
                Err(error) if error.is_block()
            ));
            installed.close(&mut driver);
        });
    assert_eq!(peer.read(&mut [0]).expect("retired connection"), 0);
}
