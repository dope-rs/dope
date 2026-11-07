pub mod common;

use std::{net, num::NonZero, rc::Rc, time::Duration};

use dope_core::{
    driver::{
        life::recovery,
        route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
    },
    io,
};
use dope_manifold::{
    app::Host,
    network::datagram::{
        plan,
        plan::Config,
        send::{Outcome, Payload},
    },
};
use dope_runtime::executor::{drive, session::Completion};
use dope_test::{fibers, harness::signals::Gate, reclaim::Reports};
use o3::mem::quota;

use crate::common::{
    Sequence,
    manifold::datagram::{budget::shutdown::*, handler, *},
    recover,
    scenario::Runtime,
};

#[test]
fn settles_queued_and_forced_owners() {
    for mode in [Mode::Queue, Mode::Flight] {
        let mut finish = Completion::new();
        let receiver =
            net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("receiver bind failed");
        let addr = receiver.local_addr().expect("receiver address unavailable");
        let payloads = State::payloads();
        let bytes = payloads.iter().map(Vec::capacity).sum();
        let queued = Gate::new();
        let audit = Rc::new(Sequence::new(
            NonZero::new(usize::from(PENDING)).expect("pending settlement capacity"),
        ));
        let runtime = Runtime::quic::<4096, 2048>();
        let mut reports =
            Reports::<_>::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

        runtime.with_scope(
            |mut session| {
                let config = Config::new(usize::from(PENDING), bytes, 1)
                    .expect("valid send bounds rejected");
                let endpoint = {
                    let mut driver = session.driver();
                    plan::Plan::<KeyTag<0>>::new(config)
                        .prepare(&mut driver)
                        .expect("endpoint preparation failed")
                        .bind(
                            (net::Ipv4Addr::LOCALHOST, 0).into(),
                            handler::Handler::new(
                                State {
                                    addr,
                                    payloads: Some(payloads),
                                    mode,
                                    queued: queued.clone(),
                                    audit: Rc::clone(&audit),
                                },
                                check_state,
                            ),
                            &mut driver,
                        )
                        .expect("endpoint bind failed")
                };
                let timeout = match mode {
                    Mode::Queue => Duration::from_secs(2),
                    Mode::Flight => Duration::ZERO,
                };
                let result =
                    session.with_quiesce(&mut finish, Host::new(endpoint), timeout, |mut app| {
                        assert_eq!(
                            fibers::TEST
                                .try_wait(&mut app, &queued, 1)
                                .expect("progress wait"),
                            fibers::Wait::Reached
                        );
                    });
                match mode {
                    Mode::Queue => assert!(result.is_ok(), "application teardown"),
                    Mode::Flight => assert!(
                        matches!(result, Err(dope_runtime::executor::session::Error::Quiesce {
                            output: (), drive, disarm: None,
                        }) if matches!(drive, drive::Error::QuiescenceTimeout))
                    ),
                }
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        match mode {
            Mode::Queue => assert!(
                reports
                    .matches_one(&Receive::<KeyTag<0>>(Parts::new(
                        SlotIndex::ZERO,
                        Epoch::INITIAL,
                    )))
                    .expect("reclaim validation")
            ),
            Mode::Flight => {
                let events = reports.events().expect("reclaim validation");
                assert_eq!(events.len(), 2, "one send and one receive remain native");
                let mut send = None;
                let mut recv = None;
                for event in events {
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
                        event => panic!("unexpected reclaimed operation: {event:?}"),
                    }
                }
                assert_ne!(
                    send, recv,
                    "send and receive retain distinct flight identities"
                );
                assert!(
                    audit.forced.get(),
                    "the single in-flight FIFO owner must retire after every queued owner"
                );
            }
        }
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
    }
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
            let Payload::Owned(payload) = payload else {
                panic!("shutdown send changed payload ownership kind");
            };
            let bytes = <[u8; 2]>::try_from(payload.as_ref())
                .expect("indexed shutdown payload has incorrect length");
            this.audit
                .record(
                    usize::from(u16::from_ne_bytes(bytes)),
                    matches!(outcome, Outcome::Shutdown),
                )
                .expect("settlement payload index");
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
        _ => panic!("unexpected datagram callback"),
    }
}
