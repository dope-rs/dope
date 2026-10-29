pub mod common;

use std::{
    convert::Infallible,
    net::{Ipv4Addr, SocketAddr},
    num::NonZero,
    time::Duration,
};

use common::{
    manifold::{
        Pattern,
        datagram::{
            Receive, handler,
            senders::{self, App, State},
        },
    },
    scenario::Runtime,
};
use dope_core::{
    dope_sys::io::socket::msg::source,
    driver::{
        life::{creation::Attempt, recovery},
        route::{Epoch, FrameworkTag, KeyTag, Tag, indices::SlotIndex, kind, table::Parts},
    },
    io::datagram::Gso,
};
use dope_manifold::network::datagram::{self, plan, plan::Config, send};
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers,
    harness::{
        peer::udp,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

#[test]
fn gso_is_one_owned_send() {
    assert!(
        datagram::GSO_LIMITS.is_some(),
        "this test requires UDP GSO support"
    );
    run_case::<send::Plain>(
        &[1000, 1000, 1000, 500],
        Ipv4Addr::LOCALHOST,
        Ipv4Addr::LOCALHOST,
        Ok,
    );
    let mut lens = vec![1000usize; 63];
    lens.push(500);
    run_case::<send::Plain>(&lens, Ipv4Addr::LOCALHOST, Ipv4Addr::LOCALHOST, Ok);
}

#[test]
fn keeps_selected_source() {
    for lens in [&[777][..], &[1000, 1000, 500][..]] {
        run_case::<send::Sourced<Gso>>(
            lens,
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::new(127, 0, 0, 2),
            senders::selected_source,
        );
    }
}

fn run_case<M: send::Segmented>(
    lens: &[usize],
    bind: Ipv4Addr,
    source: Ipv4Addr,
    address: impl FnOnce(SocketAddr) -> Result<M::Address, source::Error>,
) where
    M::Address: Copy + Unpin,
{
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let total: usize = lens.iter().sum();
    let count = (NonZero::<usize>::new(lens.len())).expect("GSO case has no segments");
    udp::Script::with(
        |socket| senders::collect(socket, count),
        |peer| {
            let dst = peer.addr();
            let address = address(dst).expect("selected datagram source");

            let queued = Gate::new();
            let received = Gate::new();
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
                    |mut sess| {
                        let want = Pattern(total).bytes();
                        let size = if lens.len() > 1 {
                            let size = u16::try_from(lens[0]).expect("GSO segment exceeds u16");
                            Some(NonZero::<u16>::new(size).expect("GSO segment is empty"))
                        } else {
                            None
                        };
                        let config = {
                            let outcome = Config::new(1, total, 1);
                            outcome.expect("GSO send bounds rejected")
                        };
                        let sender = {
                            let mut driver = sess.driver();
                            let plan = {
                                let outcome =
                                    plan::Plan::<KeyTag<0>, M>::new(config).prepare(&mut driver);
                                outcome.expect("sender preparation failed")
                            };
                            {
                                let outcome = plan.bind(
                                    (bind, 0).into(),
                                    handler::Handler::new(
                                        State::<M> {
                                            queued: queued.clone(),
                                            received: received.clone(),
                                            pending: Some((want.clone(), size, address)),
                                        },
                                        check_gso::<M>,
                                    ),
                                    &mut driver,
                                );
                                outcome.expect("sender bind failed")
                            }
                        };
                        let origin = SocketAddr::from((source, sender.local_addr().port()));
                        let app = App { sender };

                        ({
                            let outcome = sess.with_quiesce(
                                &mut finish,
                                app,
                                Duration::from_secs(2),
                                |mut app| {
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &queued, 1)
                                            .expect("progress wait"),
                                        fibers::Wait::Reached
                                    );
                                    let result =
                                        (peer.finish()).expect("UDP collector worker failed");
                                    let capture =
                                        result.expect("collect GSO packets from one source");
                                    assert_eq!(
                                        capture.source, origin,
                                        "GSO must originate from sender"
                                    );
                                    assert_eq!(
                                        capture.bytes, want,
                                        "reassembled GSO payload differs from source"
                                    );
                                },
                            );
                            outcome.expect("application teardown")
                        });
                    },
                    recovery::Policy::new(
                        |event| reports.push(event),
                        |failure: recovery::Recovery<'_, '_>| -> Infallible {
                            panic!("native recovery: {:?}", failure.cause())
                        },
                    ),
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
                received.hits(),
                0,
                "send-only GSO endpoint received a packet"
            );
        },
    )
    .expect("UDP collector spawn failed");
}

fn check_gso<'d, M: send::Segmented>(
    this: &mut State<M>,
    event: handler::Event<'_, '_, '_, '_, 'd, M>,
) where
    M::Address: Copy,
{
    match event {
        handler::Event::Access(received, _) => {
            received.expect("receive a valid datagram");
            this.received.hit();
        }
        handler::Event::Park(mut socket, _, _) => {
            let Some((payload, size, address)) = this.pending.take() else {
                return;
            };
            let retry = payload.clone();
            let original = retry.as_ptr();
            match State::<M>::queue(socket.as_mut(), (payload, size, address)) {
                Ok(_) => {
                    let rejected = State::<M>::queue(socket, (retry, size, address))
                        .err()
                        .expect("one pending send fills the queue");
                    let (retry, cause) = rejected.into_parts();
                    assert_eq!(cause, datagram::RejectCause::QueueFull);
                    assert_eq!(retry.as_ptr(), original, "rejection preserves its owner");
                    this.queued.hit();
                }
                Err(rejected) => {
                    let (payload, cause) = rejected.into_parts();
                    assert_eq!(cause, datagram::RejectCause::QueueFull);
                    this.pending = Some((payload, size, address));
                }
            }
        }
        event => assert!(
            matches!(event, handler::Event::Access(..) | handler::Event::Park(..)),
            "unexpected datagram callback"
        ),
    }
}
