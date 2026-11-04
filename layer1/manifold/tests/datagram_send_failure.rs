pub mod common;

use std::{cell::Cell, net, rc::Rc, time::Duration};

use dope_core::{
    driver::{
        life::recovery,
        retained::flight,
        route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
    },
    io::socket::install::Install,
};
use dope_manifold::app::Host;
use dope_manifold::network::{
    datagram,
    datagram::{
        plan,
        plan::Config,
        send,
        send::{Outcome, Payload},
    },
};
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers,
    harness::{
        observe::VecCellExt,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

use crate::common::{
    manifold::datagram::{handler, owners::*, *},
    recover,
    scenario::Runtime,
};

#[test]
fn settles_exact_owner() {
    let mut finish = Completion::new();
    let addr = net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 9));
    let expected = vec![
        b"send-correlation-owner-1".to_vec(),
        b"send-correlation-owner-2".to_vec(),
    ];
    let source = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("shutdown socket");
    let error = source
        .send_to(&expected[0], addr)
        .expect_err("incompatible native destination");
    let error = dope_core::Errno::try_from(error.raw_os_error().expect("native send error"))
        .expect("errno");
    let settled = Gate::new();
    let received = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let rejected = Cell::new(None);
    let runtime = Runtime::quic::<64, 2048>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    runtime.with_scope(
        |mut session| {
            let config = Config::new(1, 4096, 1).expect("single-slot send bounds rejected");
            let endpoint = {
                let mut driver = session.driver();
                let plan = plan::Plan::<KeyTag<0>>::new(config)
                    .prepare(&mut driver)
                    .expect("IPv4 sender preparation failed");

                plan.bind(
                    (net::Ipv4Addr::LOCALHOST, 0).into(),
                    handler::Handler::new(
                        FailingSend {
                            addr,
                            pending: expected.clone().into(),
                            active: Owners {
                                queued: None,
                                flight: None,
                            },
                            prior: Owners {
                                queued: None,
                                flight: None,
                            },
                            accepted: None,
                            settled: settled.clone(),
                            received: received.clone(),
                            events: Rc::clone(&events),
                        },
                        check_failingsend,
                    ),
                    &mut driver,
                )
                .expect("IPv4 sender bind failed")
            };
            let (socket, flights) = {
                let mut driver = session.driver();
                let (socket, _) = Install::adopt_udp(driver.reborrow(), source.into())
                    .expect("shutdown descriptor");
                let flights = driver
                    .storage
                    .reborrow()
                    .try_flights(flight::Need::<KeyTag<0>>::new(
                        flight::Capacity::fixed::<1>(),
                    ))
                    .expect("shutdown flights")
                    .slots();
                (socket, flights)
            };

            let outcome = session.with_quiesce(
                &mut finish,
                Host::new(completion::Observed {
                    endpoint,
                    socket,
                    flights,
                    shutdown: None,
                    completed: None,
                    rejected: &rejected,
                }),
                Duration::from_secs(2),
                |mut app| {
                    assert_eq!(
                        fibers::TEST
                            .try_wait(&mut app, &settled, 2)
                            .expect("progress wait"),
                        fibers::Wait::Reached
                    );
                },
            );
            assert!(outcome.is_ok(), "application teardown");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert_eq!(
        rejected.get(),
        Some(true),
        "unrelated native send was accepted"
    );
    assert!(
        reports
            .matches_one(&Receive::<KeyTag<0>>(Parts::new(
                SlotIndex::ZERO,
                Epoch::INITIAL,
            )))
            .expect("reclaim validation")
    );

    let events = events.take();
    assert_eq!(received.hits(), 0, "send failure fixture received a packet");
    assert_eq!(events.len(), expected.len());
    for (event, expected) in events.iter().zip(&expected) {
        assert_eq!(event.addr, addr);
        assert_eq!(event.payload.as_ref(), expected);
        let Outcome::Failed(errno) = event.outcome else {
            panic!("native send failure");
        };
        assert_eq!(errno, error);
    }
}

fn check_failingsend<'d>(this: &mut FailingSend<'d>, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Packet(received) => {
            received.expect("receive a valid datagram");
            this.received.hit();
        }
        handler::Event::Settle(event) => {
            let (owner, addr, outcome, payload, at) = event.into_parts();
            let expected = this
                .active
                .flight
                .take()
                .expect("kernel flight has an accepted owner");
            assert!(owner == send::Owner::Flight(expected));
            assert!(this.active.queued.is_none());
            let accepted = this
                .accepted
                .take()
                .expect("settlement has no queue acceptance observation");
            assert!(
                at >= accepted,
                "terminal observation preceded queue acceptance"
            );
            let Payload::Owned(payload) = payload else {
                panic!("owned send changed payload ownership kind");
            };
            this.events.push(Event {
                addr,
                outcome,
                payload,
            });
            this.settled.hit();
        }
        handler::Event::Park(mut socket, now, _) => {
            if this.active.queued.is_some() || this.active.flight.is_some() {
                return;
            }
            let Some(payload) = this.pending.pop_front() else {
                return;
            };
            match socket.as_mut().queue_vec(payload, this.addr) {
                Ok(owner) => {
                    assert!(this.accepted.replace(now).is_none());
                    assert!(this.active.queued.replace(owner).is_none());
                    assert!(this.prior.queued.replace(owner) != Some(owner));
                }
                Err(rejected) => {
                    let (payload, cause) = rejected.into_parts();
                    assert!(matches!(cause, datagram::RejectCause::QueueFull));
                    this.pending.push_front(payload);
                }
            }
        }
        handler::Event::Transfer(transfer) => {
            assert!(this.active.queued.take() == Some(transfer.queued()));
            assert!(this.active.flight.replace(transfer.flight()).is_none());
            assert!(this.prior.flight.replace(transfer.flight()) != Some(transfer.flight()));
        }
        _ => panic!("unexpected datagram callback"),
    }
}
