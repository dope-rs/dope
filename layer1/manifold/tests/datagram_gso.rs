pub mod common;

use std::{
    net::{Ipv4Addr, SocketAddr},
    num::NonZero,
    time::Duration,
};

use common::{
    manifold::{
        Pattern,
        datagram::{
            handler,
            senders::{self, State},
        },
    },
    recover,
    scenario::Runtime,
};
use dope_core::{
    dope_sys::io::socket::msg::source,
    driver::{
        life::recovery,
        route::KeyTag,
    },
    io::datagram::{Gso, GsoPayload},
};
use dope_manifold::{
    app::Host,
    network::datagram::{self, plan, plan::Config, send},
};
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers,
    harness::{
        peer::udp,
        signals::{Gate, Progress as _},
    },
};

#[test]
fn gso_admission_preserves_owner() {
    let available = datagram::GSO_LIMITS.is_some();
    let mut lens = vec![1000usize; if available { 63 } else { 1 }];
    lens.push(500);
    run_case::<send::Plain>(
        &lens,
        Ipv4Addr::LOCALHOST,
        Ipv4Addr::LOCALHOST,
        Ok,
        available,
    );
}

#[test]
fn keeps_selected_source() {
    let available = datagram::GSO_LIMITS.is_some();
    for lens in [&[777][..], &[1000, 1000, 500][..]] {
        if lens.len() > 1 && !available {
            break;
        }
        run_case::<send::Sourced<Gso>>(
            lens,
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::LOCALHOST,
            senders::selected_source,
            available,
        );
    }
}

fn run_case<M: send::Segmented>(
    lens: &[usize],
    bind: Ipv4Addr,
    source: Ipv4Addr,
    address: impl FnOnce(SocketAddr) -> Result<M::Address, source::Error>,
    available: bool,
) where
    M::Address: Copy + Unpin,
{
    let mut finish = Completion::new();
    let total: usize = lens.iter().sum();
    let count = NonZero::<usize>::new(if available { lens.len() } else { 1 })
        .expect("GSO case has no segments");
    udp::Script::with(
        |socket| senders::collect(socket, count),
        |peer| {
            let dst = peer.addr();
            let address = address(dst).expect("selected datagram source");

            let queued = Gate::new();
            let received = Gate::new();
            let runtime = Runtime::quic::<4096, 2048>();

            runtime.with_scope(
                |mut sess| {
                    let want = Pattern(total).bytes();
                    let size = if lens.len() > 1 {
                        let size = u16::try_from(lens[0]).expect("GSO segment exceeds u16");
                        Some(NonZero::<u16>::new(size).expect("GSO segment is empty"))
                    } else {
                        None
                    };
                    let config = Config::new(1, total, 1).expect("GSO send bounds rejected");
                    let sender = {
                        let mut driver = sess.driver();
                        let plan = plan::Plan::<KeyTag<0>, M>::new(config)
                            .prepare(&mut driver)
                            .expect("sender preparation failed");
                        plan.bind(
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
                        )
                        .expect("sender bind failed")
                    };
                    let origin = SocketAddr::from((source, sender.local_addr().port()));
                    let app = Host::new(sender);

                    let outcome =
                        sess.with_quiesce(&mut finish, app, Duration::from_secs(2), |mut app| {
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &queued, 1)
                                    .expect("progress wait"),
                                fibers::Wait::Reached
                            );
                            let result = peer.finish().expect("UDP collector worker failed");
                            let capture = result.expect("collect GSO packets from one source");
                            assert_eq!(capture.source, origin, "GSO must originate from sender");
                            assert_eq!(
                                capture.bytes, want,
                                "reassembled GSO payload differs from source"
                            );
                        });
                    outcome.expect("application teardown")
                },
                recovery::Policy::new(|_| {}, recover),
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
            let (payload, size) = match size {
                Some(size) if datagram::GSO_LIMITS.is_none() => {
                    let owner = payload.as_ptr();
                    let rejected = socket
                        .as_mut()
                        .queue_gso(payload, size, address)
                        .err()
                        .expect("GSO is unavailable");
                    let (payload, cause) = rejected.into_parts();
                    assert_eq!(cause, datagram::RejectCause::GsoUnavailable);
                    assert_eq!(payload.as_ptr(), owner);
                    assert_eq!(payload, retry);
                    (payload, None)
                }
                _ => (payload, size),
            };
            State::<M>::queue(socket.as_mut(), (payload, size, address))
                .unwrap_or_else(|_| panic!("empty send queue"));
            let rejected = State::<M>::queue(socket.as_mut(), (retry, size, address))
                .err()
                .expect("one pending send fills the queue");
            let (retry, cause) = rejected.into_parts();
            assert_eq!(cause, datagram::RejectCause::QueueFull);
            assert_eq!(retry.as_ptr(), original, "rejection preserves its owner");
            if let Some(size) = size {
                let retry = GsoPayload::try_new(retry, size)
                    .unwrap_or_else(|_| panic!("admitted GSO shape"));
                let count = retry.packet_count();
                let rejected = socket
                    .as_mut()
                    .queue_gso_payload(retry, address)
                    .err()
                    .expect("full queue returns the admitted proof");
                let (returned, cause) = rejected.into_parts();
                assert_eq!(cause, datagram::RejectCause::QueueFull);
                assert_eq!(returned.as_ref().as_ptr(), original);
                assert_eq!(returned.segment_size(), size);
                assert_eq!(returned.packet_count(), count);
            }
            this.queued.hit();
        }
        _ => panic!("unexpected datagram callback"),
    }
}
