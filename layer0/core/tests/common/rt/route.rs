use std::{net, ops::ControlFlow, time::Duration};

use dope_core::{
    driver::{
        self, life,
        retained::flight,
        route::{Epoch, KeyTag, indices::SlotIndex, kind},
        storage,
    },
    io::{datagram, event, event::net::recv, fd::handles::Descriptor, socket::install::Install},
};
use dope_sys::io::socket::route::{Plan, count, selector::Selector};

pub const SELECTOR: Selector = Selector::fixed::<0, 0x80, 1, 10, 5, 9, 6, 16>();

pub fn is_available(
    result: &Result<datagram::Group, datagram::Error>,
    address: net::SocketAddr,
    plan: Plan,
) -> bool {
    if std::env::consts::OS == "macos" {
        assert_eq!(
            result.as_ref().err(),
            Some(&datagram::Error::Unsupported { address, plan })
        );
        return false;
    }
    true
}

pub struct Packet {
    pub bytes: Vec<u8>,
    pub worker: Option<usize>,
    pub broadcast: bool,
}

struct Round<'g> {
    entries: Vec<(count::Index, datagram::Lease<'g>)>,
    address: net::SocketAddr,
    packets: Vec<Packet>,
    retire_zero: bool,
}

pub fn run(group: &mut datagram::Group, packets: Vec<Packet>, retire_zero: bool) {
    let address = group.local_addr();
    let op = Round {
        entries: group.entries().collect(),
        address,
        packets,
        retire_zero,
    };
    let config = super::Runtime::quic::<4, 512>()
        .file_slots(driver::settings::FileSlots::fixed::<0, 4>())
        .config();
    let mut creation = life::creation::Attempt::new();
    let driver = creation.try_driver(config).ok().expect("driver");
    let mut teardown = life::domain::Teardown::default();
    life::domain::Generation::new(driver)
        .start(&mut teardown)
        .enter((), op, life::recovery::Policy::new(|_| {}, super::recover))
        .ok()
        .expect("routed retirement");
}

impl life::Operation<(), ()> for Round<'_> {
    type Output = ();

    fn run<'a, 'p, 'd>(
        self,
        mut scope: life::Scope<'a, 'd>,
        _: storage::Loan<'a, 'p, 'd, ()>,
        _: &'a (),
    ) where
        'd: 'p,
        'p: 'a,
        'd: 'a,
        Self: 'd,
    {
        type Route = KeyTag<7>;
        let (_, mut context, mut schedule) = scope.split();
        let mut turn = schedule.begin();
        let slots = context
            .storage
            .reborrow()
            .try_flights(flight::Need::<Route>::new(flight::Capacity::fixed::<4>()))
            .expect("receive flights")
            .slots();
        let mut sockets = Vec::new();
        for (index, lease) in self.entries {
            let (socket, local) =
                Install::adopt_routed(context.reborrow(), lease).expect("routed adoption");
            assert_eq!(local, self.address);
            if self.retire_zero && index == count::Index::ZERO {
                Descriptor::from(socket).close(&mut context);
                continue;
            }
            let target = context
                .driver_ref()
                .targets::<Route>()
                .bind(
                    SlotIndex::try_from(index.into_raw() as u32).expect("route slot"),
                    Epoch::INITIAL,
                )
                .operation::<kind::Recv>();
            drop(
                slots
                    .recv_dgram(context.reborrow(), &socket, target)
                    .expect("receive admission"),
            );
            sockets.push(socket);
        }
        let loopback = if self.address.is_ipv4() {
            net::SocketAddr::from(([127, 0, 0, 1], 0))
        } else {
            net::SocketAddr::from((net::Ipv6Addr::LOCALHOST, 0))
        };
        let senders = [(); 2].map(|()| {
            let socket = net::UdpSocket::bind(loopback).expect("sender");
            socket.set_broadcast(true).expect("broadcast sender");
            socket
        });
        for packet in self.packets {
            for sender in &senders {
                let mut address = loopback;
                if packet.broadcast {
                    address.set_ip([127, 255, 255, 255].into());
                }
                address.set_port(self.address.port());
                sender
                    .send_to(&packet.bytes, address)
                    .expect("send datagram");
                let mut seen = 0;
                let deadline = std::time::Instant::now() + Duration::from_millis(100);
                loop {
                    let timeout = deadline.saturating_duration_since(std::time::Instant::now());
                    context
                        .wait(turn.reactor(), Some(timeout))
                        .expect("receive turn");
                    let dispatch = context.dispatch(turn.reactor(), |event, _| {
                        let event::Kind::Recv(done) = event.into_kind() else {
                            panic!("receive event")
                        };
                        let (target, _, outcome) = done.into_parts();
                        let recv::Outcome::Data(buffer) = outcome else {
                            panic!("receive data")
                        };
                        assert_eq!(
                            packet.worker,
                            Some(target.slot().index().get() as usize),
                            "wrong worker"
                        );
                        let datagram::Decoded::Packet { source, payload } =
                            datagram::Decoded::decode(buffer)
                        else {
                            panic!("complete datagram")
                        };
                        assert_eq!(source, sender.local_addr().expect("sender address"));
                        assert_eq!(payload.as_ref(), packet.bytes);
                        seen += 1;
                        ControlFlow::Continue(())
                    });
                    assert!(matches!(
                        dispatch,
                        driver::result::poll::Dispatch::Ready { retained: None, .. }
                    ));
                    if seen != 0 || std::time::Instant::now() >= deadline {
                        break;
                    }
                }
                assert_eq!(
                    seen,
                    usize::from(packet.worker.is_some()),
                    "missing or duplicate packet"
                );
            }
        }
        for socket in sockets {
            Descriptor::from(socket).close(&mut context);
        }
    }
}
