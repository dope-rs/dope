pub mod common;

use std::{convert::Infallible, net, num::NonZero, rc::Rc, time, time::Duration};

use dope_core::{
    driver::{
        life::{creation::Attempt, recovery},
        route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
    },
    io,
};
use dope_manifold::network::datagram::{
    plan,
    plan::Config,
    send::{Outcome, Payload},
};
use dope_runtime::executor::{drive, session::Completion};
use dope_test::{fibers, harness::signals::Gate, reclaim::Reports};
use o3::mem::quota;

use crate::common::{
    Sequence,
    manifold::datagram::{budget, budget::shutdown::*, handler, *},
    scenario::Runtime,
};

#[test]
fn settles_queued_owners() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let receiver =
        (net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0))).expect("receiver bind failed");
    let addr = (receiver.local_addr()).expect("receiver address unavailable");
    let payloads = State::payloads();
    let bytes = payloads.iter().map(Vec::capacity).sum();
    let queued = Gate::new();
    let audit = Rc::new(Sequence::new(
        NonZero::new(usize::from(PENDING)).expect("pending settlement capacity"),
    ));
    let runtime = Runtime::quic::<4096, 2048>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    {
        let result = runtime.try_executor(&mut create);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    }
    .run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let config = {
                    let outcome = Config::new(usize::from(PENDING), bytes, 1);
                    outcome.expect("valid send bounds rejected")
                };
                let endpoint = {
                    let mut driver = session.driver();
                    let plan = {
                        let outcome = plan::Plan::<KeyTag<0>>::new(config).prepare(&mut driver);
                        outcome.expect("endpoint preparation failed")
                    };

                    {
                        let outcome = plan.bind(
                            (net::Ipv4Addr::LOCALHOST, 0).into(),
                            handler::Handler::new(
                                State {
                                    addr,
                                    payloads: Some(payloads),
                                    mode: Mode::Queue,
                                    queued: queued.clone(),
                                    audit: Rc::clone(&audit),
                                },
                                check_state,
                            ),
                            &mut driver,
                        );
                        outcome.expect("endpoint bind failed")
                    }
                };

                ({
                    let outcome = session.with_quiesce(
                        &mut finish,
                        budget::App { endpoint },
                        Duration::from_secs(2),
                        |mut app| {
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &queued, 1)
                                    .expect("progress wait"),
                                fibers::Wait::Reached
                            );
                        },
                    );
                    assert!(outcome.is_ok(), "application teardown");
                });
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        result
            .as_ref()
            .map_err(|error| error.cause())
            .expect("native retirement");
        result.ok().expect("native retirement")
    });
    assert!(
        reports
            .matches_one(&Receive::<KeyTag<0>>(Parts::new(
                SlotIndex::ZERO,
                Epoch::INITIAL,
            )))
            .expect("reclaim validation")
    );

    assert_eq!(
        audit.received.get(),
        0,
        "shutdown fixture received a packet"
    );
    assert!(audit.stopped.get());
    assert_eq!(audit.settled.get(), usize::from(PENDING));
    assert!(audit.seen.iter().all(|count| count.get() == 1));
}

#[test]
fn settles_forced_owners() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let receiver =
        (net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0))).expect("receiver bind failed");
    let addr = (receiver.local_addr()).expect("receiver address unavailable");
    let payloads = State::payloads();
    let bytes = payloads.iter().map(Vec::capacity).sum();
    let queued = Gate::new();
    let audit = Rc::new(Sequence::new(
        NonZero::new(usize::from(PENDING)).expect("pending settlement capacity"),
    ));
    let runtime = Runtime::quic::<4096, 2048>();
    let mut reports =
        Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    {
        let result = runtime.try_executor(&mut create);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    }.run(|executor| {
        let result = executor.enter_with(|mut session| {
        let config = {
            let outcome = Config::new(usize::from(PENDING), bytes, 1);
            outcome.expect("valid send bounds rejected")
        };
        let endpoint = {
            let mut driver = session.driver();
            let plan = {
                let outcome = plan::Plan::<KeyTag<0>>::new(config).prepare(&mut driver);
                outcome.expect("endpoint preparation failed")
            };

            {
                let outcome = plan.bind(
                    (net::Ipv4Addr::LOCALHOST, 0).into(),
                    handler::Handler::new(State {
                        addr,
                        payloads: Some(payloads),
                        mode: Mode::Flight,
                        queued: queued.clone(),
                        audit: Rc::clone(&audit),
                    }, check_state),
                    &mut driver,
                );
                outcome.expect("endpoint bind failed")
            }
        };

        let result = session
            .with_quiesce(&mut finish, budget::App { endpoint }, time::Duration::ZERO, |mut app| {
                assert_eq!(fibers::TEST.try_wait(&mut app, &queued, 1).expect("progress wait"), fibers::Wait::Reached)
            });
        assert!(
            matches!(result, Err(dope_runtime::executor::session::Error::Quiesce { output: (), drive, disarm: None })
            if drive.kind() == drive::Kind::QuiescenceTimeout)
        );
    }, recovery::Policy::new(|event| reports.push(event), recover));
        result
            .as_ref()
            .map_err(|error| error.cause())
            .expect("native retirement");
        result.ok().expect("native retirement")
    });
    assert_eq!(
        reports.events().expect("reclaim validation").len(),
        2,
        "one send and one receive remain native"
    );
    let mut send = None;
    let mut recv = None;
    for event in reports.events().expect("reclaim validation") {
        match event {
            io::Reclaimed::Send { echo, result } => {
                assert!(
                    send.replace(echo).is_none(),
                    "the in-flight send retires once"
                );
                assert_eq!(
                    result.expect("the submitted datagram completed").into_raw(),
                    2
                );
            }
            io::Reclaimed::Recv { echo, result, flow } => {
                assert!(
                    recv.replace(echo).is_none(),
                    "the multishot receive retires once"
                );
                assert_eq!(*result, Err(dope_core::Errno::CANCELED));
                assert!(matches!(flow, dope_core::io::event::batch::Flow::Last));
            }
            event => assert!(
                matches!(
                    event,
                    io::Reclaimed::Send { .. } | io::Reclaimed::Recv { .. }
                ),
                "unexpected reclaimed operation: {event:?}"
            ),
        }
    }
    assert_ne!(
        send, recv,
        "send and receive retain distinct flight identities"
    );

    assert_eq!(
        audit.received.get(),
        0,
        "shutdown fixture received a packet"
    );
    assert!(audit.stopped.get());
    assert_eq!(audit.settled.get(), usize::from(PENDING));
    assert!(
        audit.seen.iter().all(|count| count.get() == 1),
        "every accepted owner must be settled exactly once"
    );
    assert!(
        audit.forced.get(),
        "the single in-flight FIFO owner must retire after every queued owner"
    );
}

fn check_state<'d>(this: &mut State, event: handler::Event<'_, '_, '_, '_, 'd>) {
    match event {
        handler::Event::Packet(received) => {
            received.expect("receive a valid datagram");
            let audit = &this.audit;
            audit.receive();
        }
        handler::Event::Settle(event) => {
            let (_, addr, outcome, payload, _) = event.into_parts();
            assert_eq!(addr, this.addr);
            assert!(
                matches!(&payload, Payload::Owned(_)),
                "shutdown send changed payload ownership kind"
            );
            if let Payload::Owned(payload) = payload {
                let bytes = (<[u8; 2]>::try_from(payload.as_ref()))
                    .expect("indexed shutdown payload has incorrect length");
                this.audit
                    .record(
                        usize::from(u16::from_ne_bytes(bytes)),
                        matches!(outcome, Outcome::Shutdown),
                    )
                    .expect("settlement payload index");
            }
        }
        handler::Event::Park(mut socket, _, work) => {
            let (payloads, permit) = match work.claim(|| this.payloads.take()) {
                quota::Admission::Item(item) => item,
                quota::Admission::Empty | quota::Admission::Exhausted => return,
            };
            permit.run(|| {
                for payload in payloads {
                    let outcome = socket.as_mut().queue_vec(payload, this.addr);
                    assert!(
                        outcome.is_ok(),
                        "shutdown fixture queue capacity: {:?}",
                        outcome.as_ref().err().map(|error| error.cause()),
                    );
                }
                if matches!(this.mode, Mode::Queue) {
                    while work.permit().is_some() {}
                }
                this.queued.hit();
            });
        }
        other => assert!(
            matches!(
                other,
                handler::Event::Packet(..) | handler::Event::Settle(..) | handler::Event::Park(..)
            ),
            "unexpected datagram callback"
        ),
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
