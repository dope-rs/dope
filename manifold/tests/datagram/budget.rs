use std::{
    net, pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time,
};

use dope_manifold::datagram::{
    self, Endpoint, Handler, Socket,
    packet::{Packet, Retained},
};
use dope_test::fibers::Gate;

enum Stage {
    Initial(Vec<u8>, Vec<u8>),
    Waiting(Vec<u8>),
    Done,
}

struct CapacityBound {
    destination: net::SocketAddr,
    stage: Stage,
    released: Gate,
}

impl<'d> Handler<'d, 0> for CapacityBound {
    fn packet<'turn>(
        &mut self,
        _addr: net::SocketAddr,
        _packet: Packet<'turn, 'd>,
        _socket: pin::Pin<&'turn mut Socket<'d, 0>>,
        _now: time::Instant,
    ) {
    }

    fn pre_park<'turn>(
        &mut self,
        mut socket: pin::Pin<&mut Socket<'d, 0>>,
        _now: time::Instant,
        work: dope_core::driver::schedule::Application<'turn, 'd>,
    ) {
        if !work.take() {
            return;
        }
        let stage = std::mem::replace(&mut self.stage, Stage::Done);
        match stage {
            Stage::Initial(first, second) => {
                socket
                    .as_mut()
                    .queue_to(first, self.destination)
                    .expect("first capacity fits exactly");
                let second = socket
                    .as_mut()
                    .queue_to(second, self.destination)
                    .expect_err("two resident capacities exceed the bound");
                self.stage = Stage::Waiting(second);
            }
            Stage::Waiting(payload) => {
                if let Err(payload) = socket.as_mut().queue_to(payload, self.destination) {
                    self.stage = Stage::Waiting(payload);
                    return;
                }
                self.released.hit();
            }
            stage => self.stage = stage,
        }
    }

    fn progress(
        &self,
        _region: &o3::cell::region::Token<'d>,
    ) -> dope_core::driver::schedule::Progress<'d> {
        if matches!(self.stage, Stage::Initial(..)) {
            dope_core::driver::schedule::Progress::Runnable
        } else {
            dope_core::driver::schedule::Progress::Quiescent
        }
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(
    core = dope_core,
    manifold = dope_manifold,
    runtime = dope_runtime,
    region = o3::cell::region::Token,
)]
struct App<'d> {
    #[pin]
    #[manifold]
    endpoint: Endpoint<'d, 0, CapacityBound>,
}

struct PacketBound {
    rejected: Gate,
}

impl<'d> Handler<'d, 0> for PacketBound {
    fn packet<'turn>(
        &mut self,
        source: net::SocketAddr,
        packet: Packet<'turn, 'd>,
        mut socket: pin::Pin<&'turn mut Socket<'d, 0>>,
        _now: time::Instant,
    ) {
        assert_eq!(packet.as_ref(), [1]);
        let packet = socket
            .as_mut()
            .queue_packet(packet, source)
            .expect_err("a packet retains its complete receive slot");
        assert_eq!(packet.as_ref(), [1]);
        self.rejected.hit();
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(
    core = dope_core,
    manifold = dope_manifold,
    runtime = dope_runtime,
    region = o3::cell::region::Token,
)]
struct PacketApp<'d> {
    #[pin]
    #[manifold]
    endpoint: Endpoint<'d, 0, PacketBound>,
}

struct ReceiveBound<'d> {
    retained: Option<Retained<'d>>,
    completed: Gate,
}

impl<'d> Handler<'d, 0> for ReceiveBound<'d> {
    fn packet<'turn>(
        &mut self,
        _source: net::SocketAddr,
        packet: Packet<'turn, 'd>,
        socket: pin::Pin<&'turn mut Socket<'d, 0>>,
        _now: time::Instant,
    ) {
        let Some(retained) = self.retained.take() else {
            let Ok(packet) = socket.as_ref().retain_packet(packet) else {
                panic!("first retained packet did not fit");
            };
            self.retained = Some(packet);
            return;
        };
        let Err(packet) = socket.as_ref().retain_packet(packet) else {
            panic!("second retained packet exceeded the bound");
        };
        drop(retained);
        let Ok(packet) = socket.as_ref().retain_packet(packet) else {
            panic!("dropping a retained packet did not release its credit");
        };
        self.retained = Some(packet);
        self.completed.hit();
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(
    core = dope_core,
    manifold = dope_manifold,
    runtime = dope_runtime,
    region = o3::cell::region::Token,
)]
struct ReceiveApp<'d> {
    #[pin]
    #[manifold]
    endpoint: Endpoint<'d, 0, ReceiveBound<'d>>,
}

struct RangeBound<'d> {
    retained: Option<(Retained<'d>, Retained<'d>)>,
    completed: Gate,
}

impl<'d> Handler<'d, 0> for RangeBound<'d> {
    fn packet<'turn>(
        &mut self,
        _source: net::SocketAddr,
        packet: Packet<'turn, 'd>,
        socket: pin::Pin<&'turn mut Socket<'d, 0>>,
        _now: time::Instant,
    ) {
        let packet = packet.freeze();
        let retainer = socket.as_ref().packet_retainer();
        let Some(retained) = self.retained.take() else {
            let head = packet.as_ref()[0];
            let next = packet.as_ref()[1];
            let first = retainer.retain(&packet, 0..1).expect("first range");
            let second = retainer.retain(&packet, 1..2).expect("second range");
            assert_eq!(first.as_ref(), [head]);
            assert_eq!(second.as_ref(), [next]);
            self.retained = Some((first, second));
            return;
        };
        let head = packet.as_ref()[0];
        assert!(retainer.retain(&packet, 0..1).is_none());
        drop(retained);
        let first = retainer.retain(&packet, 0..1).expect("released range");
        let reversed = std::ops::Range { start: 1, end: 0 };
        let first = match first.into_range(reversed) {
            Ok(_) => panic!("reversed retained range was accepted"),
            Err(first) => first,
        };
        let first = match first.into_range(0..1) {
            Ok(first) => first,
            Err(_) => panic!("valid retained range was rejected"),
        };
        let second = first.get(0..1).expect("shared range");
        assert_eq!(first.as_ref(), [head]);
        self.retained = Some((first, second));
        self.completed.hit();
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(
    core = dope_core,
    manifold = dope_manifold,
    runtime = dope_runtime,
    region = o3::cell::region::Token,
)]
struct RangeApp<'d> {
    #[pin]
    #[manifold]
    endpoint: Endpoint<'d, 0, RangeBound<'d>>,
}

struct ShutdownRecycle {
    destination: net::SocketAddr,
    payloads: Option<Vec<Vec<u8>>>,
    queued: Gate,
    stopped: Arc<AtomicBool>,
    recycled: Arc<AtomicUsize>,
}

impl<'d> Handler<'d, 0> for ShutdownRecycle {
    fn packet<'turn>(
        &mut self,
        _source: net::SocketAddr,
        _packet: Packet<'turn, 'd>,
        _socket: pin::Pin<&'turn mut Socket<'d, 0>>,
        _now: time::Instant,
    ) {
    }

    fn recycle(&mut self, payload: Vec<u8>) {
        self.recycled.fetch_add(1, Ordering::Relaxed);
        drop(payload);
    }

    fn pre_park<'turn>(
        &mut self,
        mut socket: pin::Pin<&mut Socket<'d, 0>>,
        _now: time::Instant,
        work: dope_core::driver::schedule::Application<'turn, 'd>,
    ) {
        let Some(payloads) = self.payloads.take() else {
            return;
        };
        if !work.take() {
            self.payloads = Some(payloads);
            return;
        }
        for payload in payloads {
            socket
                .as_mut()
                .queue_to(payload, self.destination)
                .expect("pending payload");
        }
        while work.take() {}
        self.queued.hit();
    }

    fn progress(
        &self,
        _region: &o3::cell::region::Token<'d>,
    ) -> dope_core::driver::schedule::Progress<'d> {
        if self.payloads.is_some() {
            dope_core::driver::schedule::Progress::Runnable
        } else {
            dope_core::driver::schedule::Progress::Quiescent
        }
    }

    fn shutdown(&mut self) {
        self.stopped.store(true, Ordering::Release);
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(
    core = dope_core,
    manifold = dope_manifold,
    runtime = dope_runtime,
    region = o3::cell::region::Token,
)]
struct ShutdownRecycleApp<'d> {
    #[pin]
    #[manifold]
    endpoint: Endpoint<'d, 0, ShutdownRecycle>,
}

#[test]
fn resident_limit_counts_capacity_and_completion_releases_it() {
    let receiver = net::UdpSocket::bind("127.0.0.1:0").expect("receiver");
    let destination = receiver.local_addr().expect("receiver address");
    let mut first = Vec::with_capacity(4096);
    first.push(1);
    let mut second = Vec::with_capacity(4096);
    second.push(2);
    let retained_send_bytes = first.capacity().max(second.capacity());
    assert!(first.len() + second.len() <= retained_send_bytes);
    assert!(first.capacity() + second.capacity() > retained_send_bytes);

    let released = Gate::new();
    dope_test::scenario::rt::Runtime::quic(4096, 2048)
        .executor()
        .enter(|mut session| {
            let config =
                datagram::Config::new(2, retained_send_bytes, 2).expect("valid send bounds");
            let endpoint = Endpoint::bind_with_config(
                "127.0.0.1:0".parse().expect("bind address"),
                CapacityBound {
                    destination,
                    stage: Stage::Initial(first, second),
                    released: released.clone(),
                },
                config,
                &mut session.driver_access(),
            )
            .expect("bind endpoint");

            session
                .with_app(App { endpoint }, |mut app| {
                    dope_test::fibers::TEST.run_until(&mut app, &released, 1);
                })
                .expect("application teardown");
        });
}

#[test]
fn packet_charges_the_complete_receive_slot() {
    const RECEIVE_PAYLOAD_BYTES: u32 = 2048;

    let rejected = Gate::new();
    let runtime = dope_test::scenario::rt::Runtime::quic(4096, RECEIVE_PAYLOAD_BYTES);
    let receive_slot_bytes = runtime.config().receive().buffer_len();
    runtime.executor().enter(|mut session| {
        let config =
            datagram::Config::new(1, receive_slot_bytes - 1, 1).expect("valid send bounds");
        let endpoint = Endpoint::bind_with_config(
            "127.0.0.1:0".parse().expect("bind address"),
            PacketBound {
                rejected: rejected.clone(),
            },
            config,
            &mut session.driver_access(),
        )
        .expect("bind endpoint");
        let destination = endpoint.local_addr();
        let sender = net::UdpSocket::bind("127.0.0.1:0").expect("sender");
        sender.send_to(&[1], destination).expect("send packet");

        session
            .with_app(PacketApp { endpoint }, |mut app| {
                dope_test::fibers::TEST.run_until(&mut app, &rejected, 1);
            })
            .expect("application teardown");
    });
}

#[test]
fn retained_packets_are_explicitly_bounded() {
    let completed = Gate::new();
    let runtime = dope_test::scenario::rt::Runtime::quic(4096, 2048);
    let receive_slot_bytes = runtime.config().receive().buffer_len();
    runtime.executor().enter(|mut session| {
        let config = datagram::Config::new(2, 4096, 2)
            .expect("valid send bounds")
            .with_retained_receive_bytes(receive_slot_bytes);
        let endpoint = Endpoint::bind_with_config(
            "127.0.0.1:0".parse().expect("bind address"),
            ReceiveBound {
                retained: None,
                completed: completed.clone(),
            },
            config,
            &mut session.driver_access(),
        )
        .expect("bind endpoint");
        let destination = endpoint.local_addr();
        let sender = net::UdpSocket::bind("127.0.0.1:0").expect("sender");
        sender.send_to(&[1], destination).expect("first packet");
        sender.send_to(&[2], destination).expect("second packet");

        session
            .with_app(ReceiveApp { endpoint }, |mut app| {
                dope_test::fibers::TEST.run_until(&mut app, &completed, 1);
            })
            .expect("application teardown");
    });
}

#[test]
fn retained_ranges_share_the_packet_charge() {
    let completed = Gate::new();
    let runtime = dope_test::scenario::rt::Runtime::quic(4096, 2048);
    let receive_slot_bytes = runtime.config().receive().buffer_len();
    runtime.executor().enter(|mut session| {
        let config = datagram::Config::new(2, 4096, 2)
            .expect("valid send bounds")
            .with_retained_receive_bytes(receive_slot_bytes);
        let endpoint = Endpoint::bind_with_config(
            "127.0.0.1:0".parse().expect("bind address"),
            RangeBound {
                retained: None,
                completed: completed.clone(),
            },
            config,
            &mut session.driver_access(),
        )
        .expect("bind endpoint");
        let destination = endpoint.local_addr();
        let sender = net::UdpSocket::bind("127.0.0.1:0").expect("sender");
        sender
            .send_to(&[1, 2, 3], destination)
            .expect("first packet");
        sender
            .send_to(&[4, 5, 6], destination)
            .expect("second packet");

        session
            .with_app(RangeApp { endpoint }, |mut app| {
                dope_test::fibers::TEST.run_until(&mut app, &completed, 1);
            })
            .expect("application teardown");
    });
}

#[test]
fn shutdown_recycles_every_queued_owned_payload() {
    const PENDING: usize = 257;

    let receiver = net::UdpSocket::bind("127.0.0.1:0").expect("receiver");
    let destination = receiver.local_addr().expect("receiver address");
    let payloads: Vec<Vec<u8>> = (0..PENDING).map(|index| vec![index as u8]).collect();
    let retained_send_bytes = payloads.iter().map(Vec::capacity).sum();
    let queued = Gate::new();
    let stopped = Arc::new(AtomicBool::new(false));
    let recycled = Arc::new(AtomicUsize::new(0));

    dope_test::scenario::rt::Runtime::quic(4096, 2048)
        .executor()
        .enter(|mut session| {
            let config =
                datagram::Config::new(PENDING, retained_send_bytes, 1).expect("valid send bounds");
            let endpoint = Endpoint::bind_with_config(
                "127.0.0.1:0".parse().expect("bind address"),
                ShutdownRecycle {
                    destination,
                    payloads: Some(payloads),
                    queued: queued.clone(),
                    stopped: Arc::clone(&stopped),
                    recycled: Arc::clone(&recycled),
                },
                config,
                &mut session.driver_access(),
            )
            .expect("bind endpoint");

            session
                .with_app(ShutdownRecycleApp { endpoint }, |mut app| {
                    dope_test::fibers::TEST.run_until(&mut app, &queued, 1);
                })
                .expect("application teardown");
        });

    assert!(stopped.load(Ordering::Acquire));
    assert_eq!(recycled.load(Ordering::Relaxed), PENDING);
}
