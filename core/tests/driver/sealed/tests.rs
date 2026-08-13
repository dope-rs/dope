//! Driver contract tests hosted by the integration target's proof boundary.

use std::{
    collections::HashMap,
    io::Write as _,
    mem::size_of,
    net::UdpSocket,
    os::fd::AsFd as _,
    pin::pin,
    time::{Duration, Instant},
};

use dope_core::{
    driver,
    driver::{
        Driver,
        lifecycle::routing::Route,
        ops::{self, OutboundReservation},
        route::{
            EPOCH_MASK, Epoch, KeyTag, Operation, SLOT_MASK, SlotIndex, Target, Token, kind,
            table::{Capacity, CellSlab, ConnectionCapacity, Parts, Slab},
        },
        schedule::{
            self,
            ready::{
                completion::{self, Waker},
                task,
            },
            reservation,
            timer::Registration,
        },
        settings::{self, Profile},
    },
    io::{
        recv::{Lease, View},
        socket::option::{Stream, StreamOptions, Tuning},
        transfer,
    },
    platform::wake,
};
use dope_test::{checks::panics::Expectation, scenario::rt::Runtime};
use o3::collections::batch::set;

const ROUTE: u8 = 7;

fn wait_tuning<'d>(
    driver: &mut driver::Context<'_, 'd>,
    turn: &mut schedule::ActiveTurn<'_, 'd>,
) -> dope_core::io::event::tuning::Completion {
    loop {
        ops::poll::Poll::wait(driver, turn.reactor(), Some(Duration::from_secs(1)))
            .expect("wait tuning completion");
        let dispatched = ops::poll::Source::dispatch(driver, turn.reactor(), |event, _driver| {
            std::ops::ControlFlow::Break(event)
        });
        let (_, event) = dispatched.into_parts();
        let Some(event) = event else {
            continue;
        };
        let dope_core::io::event::Kind::Tuning(completion) = event.into_kind() else {
            panic!("expected tuning completion");
        };
        return completion;
    }
}

fn tuning_target<'d, const ID: u8>(
    driver: driver::Reference<'d>,
    slot: SlotIndex,
) -> Target<'d, KeyTag<ID>> {
    driver
        .targets::<KeyTag<ID>>()
        .bind_parts(Parts::from_components(slot, Epoch::INITIAL))
}

fn submit_tuning<'d, const ID: u8>(
    driver: &mut driver::Context<'_, 'd>,
    socket: impl Into<dope_core::io::fd::handles::Descriptor<'d>>,
    options: StreamOptions,
    target: Target<'d, KeyTag<ID>>,
) -> Tuning<'d> {
    ops::Control::submit_tuning(driver, target.bind(socket.into()), options)
        .unwrap_or_else(|_| panic!("submit setsockopt"))
}

fn stream_options(option: Stream) -> StreamOptions {
    [Some(option)].try_into().expect("valid stream option")
}

#[test]
fn retained_receive_buffers_do_not_expand_the_hot_metadata() {
    assert_eq!(size_of::<Lease<'static>>(), size_of::<View<'static>>(),);
}

#[test]
fn validated_receive_layout_fits_in_one_machine_word() {
    assert_eq!(size_of::<settings::Receive>(), size_of::<u64>());
    assert_eq!(
        size_of::<dope_core::io::fd::handles::DatagramDescriptor<'static>>(),
        size_of::<dope_core::io::fd::handles::Descriptor<'static>>()
    );
}

#[test]
fn validated_stream_options_fit_in_three_machine_words_or_less() {
    assert!(size_of::<StreamOptions>() <= 3 * size_of::<u64>());
}

#[test]
fn closing_a_reserved_descriptor_returns_its_fixed_slot() {
    Runtime::quic(2, 8).with_driver(|mut driver| {
        let (first, _) = ops::Bootstrap::bind_datagram_slot(
            &mut driver,
            "127.0.0.1:0".parse().expect("first address"),
        )
        .expect("first socket");
        let slot = first.index();
        ops::Files::close(&mut driver, first);

        let (second, _) = ops::Bootstrap::bind_datagram_slot(
            &mut driver,
            "127.0.0.1:0".parse().expect("second address"),
        )
        .expect("second socket");
        assert_eq!(second.index(), slot);
        ops::Files::close(&mut driver, second);
    });
}

#[test]
fn stale_fixed_ready_authority_cannot_retarget_or_wake_a_reused_slot() {
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        crate::with_turn(scope, |mut driver, turn| {
            let reference = driver.driver_ref();
            let old = tuning_target::<ROUTE>(reference, SlotIndex::ZERO);
            let new = tuning_target::<ROUTE>(reference, SlotIndex::from(1_u16));

            let (first, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("first address"),
            )
            .expect("first socket");
            let physical = first.index();
            let stale = first.ready_handle();
            stale.set_target(old.dispatch());
            let stale_target = stale.target();
            ops::Files::close(&mut driver, first);

            let (second, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("second address"),
            )
            .expect("second socket");
            assert_eq!(second.index(), physical);
            let current = second.ready_handle();
            current.set_target(new.dispatch());

            stale.set_target(old.dispatch());
            stale_target.wake();
            assert_eq!(turn.drain_ready(usize::MAX, drop), 0);

            current.activate();
            let mut activated = Vec::new();
            assert_eq!(
                turn.drain_ready(usize::MAX, |target| activated.push(target)),
                1
            );
            assert_eq!(activated.len(), 1);
            assert!(new.dispatch().matches(activated[0]));
            ops::Files::close(&mut driver, second);
        });
    });
}

#[test]
fn dropped_reserved_descriptor_returns_its_fixed_slot_on_a_reactor_turn() {
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        crate::with_turn(scope, |mut driver, turn| {
            let (first, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("first address"),
            )
            .expect("first socket");
            let slot = first.index();
            drop(first);
            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor())
                    .expect("reclaim dropped descriptor"),
                ops::poll::Commit::Drained
            );

            let (second, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("second address"),
            )
            .expect("second socket");
            assert_eq!(second.index(), slot);
            ops::Files::close(&mut driver, second);
        });
    });
}

#[test]
fn close_retires_recv_before_raw_fd_reuse() {
    if std::env::consts::OS != "macos" {
        return;
    }
    Runtime::quic(2, 8).with_retained_turn(|mut turn, mut driver| {
        let before = crate::open_fds();
        let (old, old_addr) = ops::Bootstrap::bind_datagram_slot(
            driver.driver(),
            "127.0.0.1:0".parse().expect("old address"),
        )
        .expect("old socket");
        let old_slot = old.index();
        let opened = crate::open_fds()
            .into_iter()
            .filter(|fd| !before.contains(fd))
            .collect::<Vec<_>>();
        assert_eq!(opened.len(), 1);
        let reused = opened[0];
        let old_target = old
            .driver()
            .targets::<KeyTag<1>>()
            .bind_parts(Parts::from_components(SlotIndex::ZERO, Epoch::INITIAL));
        crate::submit_recv(&mut driver, &old, old_target).expect("arm old recv");

        let peer = UdpSocket::bind("127.0.0.1:0").expect("peer");
        peer.send_to(&[0x41; 32], old_addr).expect("send old");
        ops::poll::Poll::wait(
            driver.driver(),
            turn.reactor(),
            Some(Duration::from_secs(1)),
        )
        .expect("receive old");

        ops::Files::close(driver.driver(), old);
        assert!(!crate::is_open(reused));

        let (new, new_addr) = ops::Bootstrap::bind_datagram_slot(
            driver.driver(),
            "127.0.0.1:0".parse().expect("new address"),
        )
        .expect("new socket");
        assert_eq!(new.index(), old_slot);
        assert!(crate::is_open(reused));
        let new_target = new
            .driver()
            .targets::<KeyTag<2>>()
            .bind_parts(Parts::from_components(SlotIndex::ZERO, Epoch::INITIAL));
        crate::submit_recv(&mut driver, &new, new_target).expect("arm new recv");
        peer.send_to(b"new", new_addr).expect("send new");
        ops::poll::Poll::wait(
            driver.driver(),
            turn.reactor(),
            Some(Duration::from_secs(1)),
        )
        .expect("receive new");

        let mut completion = None;
        let _ = crate::dispatch_all(driver.driver(), turn.reactor(), |event| {
            assert!(completion.replace(event).is_none());
        });
        let completion = completion.expect("completion");
        assert!(
            new_target
                .operation(kind::RECV)
                .matches(completion.token().expect("receive target"))
        );
        assert!(matches!(
            completion.kind(),
            dope_core::io::event::Kind::Recv(..)
        ));

        ops::Files::close(driver.driver(), new);
    });
}

#[test]
fn kqueue_commit_defers_changelist_work_beyond_one_reactor_budget() {
    if std::env::consts::OS != "macos" {
        return;
    }
    const REGISTRATIONS: u16 = 200;

    Runtime::throughput().with_retained_turn(|mut turn, mut driver| {
        let mut sockets = Vec::with_capacity(REGISTRATIONS as usize);
        for index in 0..REGISTRATIONS {
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                driver.driver(),
                "127.0.0.1:0".parse().expect("address"),
            )
            .expect("bind datagram");
            let target = socket
                .driver()
                .targets::<KeyTag<1>>()
                .bind_parts(Parts::from_components(
                    SlotIndex::from(index),
                    Epoch::INITIAL,
                ));
            crate::submit_recv(&mut driver, &socket, target)
                .expect("queue kqueue read registration");
            sockets.push(socket);
        }

        assert_eq!(
            ops::poll::Poll::commit(driver.driver(), turn.reactor())
                .expect("commit one bounded changelist batch"),
            ops::poll::Commit::Pending
        );
        assert_eq!(
            ops::poll::Poll::commit(driver.driver(), turn.reactor())
                .expect("commit remaining changelist batch"),
            ops::poll::Commit::Drained
        );

        for socket in sockets {
            ops::Files::close(driver.driver(), socket);
        }
    });
}

#[test]
fn uring_commit_reports_deferred_maintenance_across_reactor_turns() {
    if std::env::consts::OS != "linux" {
        return;
    }

    Runtime::quic(2, 8).with_driver_scope(|scope| {
        crate::with_controller(scope, |mut driver, mut controller| {
            let address = "127.0.0.1:0".parse().expect("address");
            let (first, _) = ops::Bootstrap::bind_datagram_slot(&mut driver, address)
                .expect("first datagram slot");
            let (second, _) = ops::Bootstrap::bind_datagram_slot(&mut driver, address)
                .expect("second datagram slot");
            drop((first, second));

            let mut turn = controller.begin(1);
            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor())
                    .expect("commit one retirement"),
                ops::poll::Commit::Pending
            );
            drop(turn);

            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor())
                    .expect("commit remaining maintenance"),
                ops::poll::Commit::Drained
            );
            drop(turn);
        });
    });
}

#[test]
fn receive_completion_moves_the_buffer_into_a_domain_lease() {
    Runtime::quic(2, 8).with_retained_turn(|mut turn, mut driver| {
        let (socket, address) = ops::Bootstrap::bind_datagram_slot(
            driver.driver(),
            "127.0.0.1:0".parse().expect("address"),
        )
        .expect("bind datagram");
        let target = socket
            .driver()
            .targets::<KeyTag<1>>()
            .bind_parts(Parts::from_components(SlotIndex::ZERO, Epoch::INITIAL));
        crate::submit_recv(&mut driver, &socket, target).expect("arm recv");

        let peer = UdpSocket::bind("127.0.0.1:0").expect("peer");
        peer.send_to(b"typed", address).expect("send");
        ops::poll::Poll::wait(
            driver.driver(),
            turn.reactor(),
            Some(Duration::from_secs(1)),
        )
        .expect("receive");

        let mut event = None;
        let _ = crate::dispatch_all(driver.driver(), turn.reactor(), |completion| {
            assert!(event.replace(completion).is_none());
        });
        let event = event.expect("completion").into_kind();
        let dope_core::io::event::Kind::Recv(completion) = event else {
            panic!("expected receive completion");
        };
        let (_, _, dope_core::io::RecvEvent::Data(buffer)) = completion.into_parts() else {
            panic!("expected receive buffer");
        };
        assert_eq!(buffer.as_slice(), b"typed");
        drop(buffer);
        ops::Files::close(driver.driver(), socket);
    });
}

#[test]
fn direct_source_dispatch_retains_only_the_blocking_event() {
    Runtime::quic(2, 8).with_retained_turn(|mut turn, mut driver| {
        let (socket, address) = ops::Bootstrap::bind_datagram_slot(
            driver.driver(),
            "127.0.0.1:0".parse().expect("address"),
        )
        .expect("bind datagram");
        let (spare, _) = ops::Bootstrap::bind_datagram_slot(
            driver.driver(),
            "127.0.0.1:0".parse().expect("spare address"),
        )
        .expect("bind spare datagram");
        let target = socket
            .driver()
            .targets::<KeyTag<1>>()
            .bind_parts(Parts::from_components(SlotIndex::ZERO, Epoch::INITIAL));
        crate::submit_recv(&mut driver, &socket, target).expect("arm recv");

        let peer = UdpSocket::bind("127.0.0.1:0").expect("peer");
        peer.send_to(b"direct", address).expect("send");
        ops::poll::Poll::wait(
            driver.driver(),
            turn.reactor(),
            Some(Duration::from_secs(1)),
        )
        .expect("receive");

        let mut calls = 0;
        let mut spare = Some(spare);
        let dispatched =
            ops::poll::Source::dispatch(driver.driver(), turn.reactor(), |event, driver| {
                calls += 1;
                ops::Files::close(driver, spare.take().expect("single callback"));
                std::ops::ControlFlow::Break(event)
            });
        let (drain, retained) = dispatched.into_parts();
        assert_eq!(calls, 1);
        assert!(spare.is_none());
        assert_eq!(drain, ops::poll::Drain::Pending);

        let event = retained.expect("blocking event").into_kind();
        let dope_core::io::event::Kind::Recv(completion) = event else {
            panic!("expected receive completion");
        };
        let (_, _, dope_core::io::RecvEvent::Data(buffer)) = completion.into_parts() else {
            panic!("expected receive buffer");
        };
        assert_eq!(buffer.as_slice(), b"direct");
        drop(buffer);
        ops::Files::close(driver.driver(), socket);
    });
}

#[test]
fn socket_tuning_completion_is_public() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        crate::with_turn(scope, |mut driver, turn| {
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("address"),
            )
            .expect("bind datagram");

            let owner = tuning_target::<1>(driver.driver_ref(), SlotIndex::ZERO);
            let target = owner.operation(kind::TUNING);
            let tuning = submit_tuning(
                &mut driver,
                socket,
                stream_options(Stream::Buffer(4096)),
                owner,
            );
            let Tuning::Pending(establishment) = tuning else {
                panic!("io_uring tuning must be asynchronous");
            };

            let completion = wait_tuning(&mut driver, turn);
            assert!(target.matches(completion.token()));
            let Ok((socket, event)) = establishment.complete_tuning(completion) else {
                panic!("completion must match its tuning authority");
            };
            assert!(matches!(
                event,
                dope_core::io::event::tuning::Outcome::Applied
            ));
            ops::Files::close(&mut driver, socket);
        });
    });
}

#[test]
fn socket_tuning_cancellation_keeps_the_slot_transactional() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        crate::with_turn(scope, |mut driver, turn| {
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("address"),
            )
            .expect("bind datagram");
            let owner = tuning_target::<1>(driver.driver_ref(), SlotIndex::ZERO);
            let target = owner.operation(kind::TUNING);
            let options = stream_options(Stream::Buffer(4096));
            let Tuning::Pending(establishment) = submit_tuning(&mut driver, socket, options, owner)
            else {
                panic!("io_uring tuning must be asynchronous");
            };
            let establishment = ops::Control::cancel_tuning(&mut driver, establishment)
                .map_err(|(_, error)| error)
                .expect("submit typed tuning cancellation");

            let completion = wait_tuning(&mut driver, turn);
            assert!(target.matches(completion.token()));
            let Ok((socket, _)) = establishment.complete_tuning(completion) else {
                panic!("completion must match its cancellation authority");
            };
            let Tuning::Pending(reused) = submit_tuning(&mut driver, socket, options, owner) else {
                panic!("io_uring tuning must be asynchronous");
            };
            let completion = wait_tuning(&mut driver, turn);
            let Ok((socket, _)) = reused.complete_tuning(completion) else {
                panic!("completion must match the reused authority");
            };
            ops::Files::close(&mut driver, socket);
        });
    });
}

#[test]
fn completed_tuning_reuses_its_fixed_slot() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        crate::with_turn(scope, |mut driver, turn| {
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("address"),
            )
            .expect("bind datagram");
            let options = stream_options(Stream::Buffer(4096));
            let first_owner = tuning_target::<1>(driver.driver_ref(), SlotIndex::ZERO);
            let first = first_owner.operation(kind::TUNING);
            let Tuning::Pending(first_establishment) =
                submit_tuning(&mut driver, socket, options, first_owner)
            else {
                panic!("io_uring tuning must be asynchronous");
            };
            let completion = wait_tuning(&mut driver, turn);
            assert!(first.matches(completion.token()));
            let Ok((socket, _)) = first_establishment.complete_tuning(completion) else {
                panic!("first completion must match its authority");
            };
            let second_owner = tuning_target::<2>(driver.driver_ref(), SlotIndex::ZERO);
            let second = second_owner.operation(kind::TUNING);
            let Tuning::Pending(second_establishment) =
                submit_tuning(&mut driver, socket, options, second_owner)
            else {
                panic!("io_uring tuning must be asynchronous");
            };
            let completion = wait_tuning(&mut driver, turn);
            assert!(second.matches(completion.token()));
            let Ok((socket, _)) = second_establishment.complete_tuning(completion) else {
                panic!("reused completion must match its new authority");
            };
            ops::Files::close(&mut driver, socket);
        });
    });
}

#[test]
fn socket_tuning_preserves_kernel_failure() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        crate::with_turn(scope, |mut driver, turn| {
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("address"),
            )
            .expect("bind datagram");
            let owner = tuning_target::<1>(driver.driver_ref(), SlotIndex::ZERO);
            let Tuning::Pending(establishment) = submit_tuning(
                &mut driver,
                socket,
                stream_options(Stream::NoDelay(true)),
                owner,
            ) else {
                panic!("io_uring tuning must be asynchronous");
            };

            let completion = wait_tuning(&mut driver, turn);
            let Ok((socket, event)) = establishment.complete_tuning(completion) else {
                panic!("failure completion must match its authority");
            };
            assert!(matches!(
                event,
                dope_core::io::event::tuning::Outcome::Failed(_)
            ));
            let owner = tuning_target::<2>(driver.driver_ref(), SlotIndex::ZERO);
            let Tuning::Pending(reused) = submit_tuning(
                &mut driver,
                socket,
                stream_options(Stream::Buffer(4096)),
                owner,
            ) else {
                panic!("failed tuning must release its fixed-slot transaction");
            };
            let completion = wait_tuning(&mut driver, turn);
            let Ok((socket, event)) = reused.complete_tuning(completion) else {
                panic!("reused completion must match its tuning authority");
            };
            assert!(matches!(
                event,
                dope_core::io::event::tuning::Outcome::Applied
            ));
            ops::Files::close(&mut driver, socket);
        });
    });
}

#[test]
fn setsockopt_capacity_tracks_distinct_fixed_slots() {
    if std::env::consts::OS != "linux" {
        return;
    }
    const SUBMISSIONS: usize = 193;
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_queue_layout(settings::QueueLayout::fixed::<64, 128>())
        .with_file_slots(settings::FileSlots::fixed::<0, 256>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |mut driver, turn| {
            let mut sockets = Vec::with_capacity(SUBMISSIONS);
            for _ in 0..SUBMISSIONS {
                let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                    &mut driver,
                    "127.0.0.1:0".parse().expect("address"),
                )
                .expect("bind datagram");
                sockets.push(socket);
            }
            let options = stream_options(Stream::Buffer(4096));
            let mut establishments = HashMap::with_capacity(SUBMISSIONS);
            for socket in sockets {
                let owner = tuning_target::<1>(driver.driver_ref(), socket.token_index());
                let target = owner.operation(kind::TUNING);
                let Tuning::Pending(establishment) =
                    submit_tuning(&mut driver, socket, options, owner)
                else {
                    panic!("io_uring tuning must be asynchronous");
                };
                assert!(
                    establishments
                        .insert(target.slot(), (target, establishment))
                        .is_none()
                );
            }

            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor())
                    .expect("commit tuning submissions"),
                ops::poll::Commit::Drained
            );
            while !establishments.is_empty() {
                let completion = wait_tuning(&mut driver, turn);
                let completion_target = completion.token();
                let (target, establishment) = establishments
                    .remove(&completion_target.slot())
                    .expect("completion must address one live transaction");
                assert!(target.matches(completion_target));
                let Ok((socket, _)) = establishment.complete_tuning(completion) else {
                    panic!("batch completion must match its authority");
                };
                ops::Files::close(&mut driver, socket);
            }
        });
    });
}

#[test]
fn setsockopt_reuses_the_descriptor_after_completion() {
    if std::env::consts::OS != "linux" {
        return;
    }
    Runtime::quic(2, 8).with_driver_scope(|scope| {
        crate::with_turn(scope, |mut driver, turn| {
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut driver,
                "127.0.0.1:0".parse().expect("address"),
            )
            .expect("bind datagram");
            let owner = tuning_target::<1>(driver.driver_ref(), SlotIndex::ZERO);
            let options = stream_options(Stream::Buffer(4096));

            let Tuning::Pending(first) = submit_tuning(&mut driver, socket, options, owner) else {
                panic!("io_uring tuning must be asynchronous");
            };

            let completion = wait_tuning(&mut driver, turn);
            let Ok((socket, _)) = first.complete_tuning(completion) else {
                panic!("first completion must match its authority");
            };
            let Tuning::Pending(second) = submit_tuning(&mut driver, socket, options, owner) else {
                panic!("io_uring tuning must be asynchronous");
            };
            let completion = wait_tuning(&mut driver, turn);
            let Ok((socket, _)) = second.complete_tuning(completion) else {
                panic!("second completion must match its authority");
            };
            ops::Files::close(&mut driver, socket);
        });
    });
}

#[test]
fn bootstrap_does_not_consume_unrelated_completion() {
    if std::env::consts::OS != "linux" {
        return;
    }
    let (source, notify) = wake::Pair::nonblocking().expect("shutdown wake").split();
    let mut notify = std::fs::File::from(notify);
    let driver = Driver::new(Runtime::quic(2, 8).config()).expect("driver");
    let domain = driver::lifecycle::Domain::new(driver)
        .fd(source, |source| source.as_fd())
        .expect("register shutdown");
    let owner = crate::owner();
    domain
        .enter(owner, (), |mut scope, _storage, _source| {
            crate::with_turn(&mut scope, |mut driver, turn| {
                notify.write_all(&[1]).expect("notify shutdown");
                ops::poll::Poll::wait(&mut driver, turn.reactor(), Some(Duration::from_secs(1)))
                    .expect("wait shutdown");

                let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                    &mut driver,
                    "127.0.0.1:0".parse().expect("address"),
                )
                .expect("bootstrap bind");

                let mut completion = None;
                let _ = crate::dispatch_all(&mut driver, turn.reactor(), |event| {
                    assert!(completion.replace(event).is_none());
                });
                assert!(completion.expect("shutdown completion").is_shutdown());
                ops::Files::close(&mut driver, socket);
            });
        })
        .expect("infallible storage");
}

fn target<'d>(driver: driver::Reference<'d>) -> Operation<'d, KeyTag<ROUTE>> {
    driver
        .targets::<KeyTag<ROUTE>>()
        .bind_parts(Parts::from_components(SlotIndex::ZERO, Epoch::INITIAL))
        .dispatch()
}

fn with_ready_capacity(config: settings::Config, slots: usize) -> settings::Config {
    let ready = settings::ScheduleCapacity::new(slots).expect("valid ready capacity");
    config.with_scheduler(config.scheduler().with_ready(ready))
}

fn with_timer_cache_limit(config: settings::Config, slots: usize) -> settings::Config {
    let limit = settings::ScheduleCapacity::new(slots).expect("valid timer cache limit");
    config.with_scheduler(config.scheduler().with_timer_cache_limit(limit))
}

#[test]
fn tokens_preserve_wide_logical_identity_across_kinds() {
    let parts = Parts::<KeyTag<ROUTE>>::from_components(SlotIndex::ZERO, Epoch::INITIAL);
    let target = Token::from_parts(parts);
    assert_eq!(target.epoch(), Some(Epoch::INITIAL));
    assert_eq!(parts.epoch(), Epoch::INITIAL);
    assert_eq!(Token::from_parts(parts), target);
    let tagged = target.with_kind(kind::SEND);
    assert_eq!(tagged.with_kind(0), target);
    assert!(tagged.same_target(target));

    assert_eq!(Token::framework(SlotIndex::ZERO).epoch(), None);
    assert!(Epoch::new(0).is_none());
}

#[test]
fn slot_indices_validate_at_the_raw_boundary() {
    let first_overflow = SLOT_MASK as u32 + 1;
    assert!(SlotIndex::try_new(SLOT_MASK as u32).is_some());
    assert!(SlotIndex::try_new(first_overflow).is_none());

    let indexed = Capacity::new(SLOT_MASK as usize).expect("sentinel capacity");
    assert_eq!(indexed.raw(), SLOT_MASK as u32);
    assert_eq!(
        SlotIndex::try_new(indexed.raw()),
        SlotIndex::try_new(SLOT_MASK as u32)
    );
    assert_eq!(
        indexed.slots().next_back(),
        SlotIndex::try_new(SLOT_MASK as u32 - 1)
    );

    let full = Capacity::new(SLOT_MASK as usize + 1).expect("full token capacity");
    assert_eq!(full.raw(), SLOT_MASK as u32 + 1);
    assert!(SlotIndex::try_new(full.raw()).is_none());
    assert_eq!(
        full.slots().next_back(),
        SlotIndex::try_new(SLOT_MASK as u32)
    );

    assert!(ConnectionCapacity::new(0).is_none());
    let connections =
        ConnectionCapacity::new(SLOT_MASK as usize).expect("maximum connection capacity");
    assert_eq!(connections.get(), SLOT_MASK as usize);
    assert_eq!(connections.raw(), SLOT_MASK as u32);
    assert_eq!(connections.table(), indexed);
    assert_eq!(
        connections.sentinel(),
        SlotIndex::try_new(SLOT_MASK as u32).unwrap()
    );
    assert!(ConnectionCapacity::new(SLOT_MASK as usize + 1).is_none());
    assert_eq!(size_of::<ConnectionCapacity>(), size_of::<Capacity>());
}

#[test]
fn epochs_advance_without_crossing_the_encoded_range() {
    assert_eq!(Epoch::INITIAL.next(), Epoch::new(2));
    let max = Epoch::new(EPOCH_MASK).expect("epoch mask");
    assert_eq!(max.raw(), EPOCH_MASK);
    assert_eq!(max.next(), None);
}

#[test]
fn fixed_file_layout_rejects_unencodable_shapes() {
    assert_eq!(settings::FileSlots::new(SLOT_MASK as u32 + 2, 0), None);
    assert_eq!(settings::FileSlots::new(16, u32::MAX), None);
    let split = settings::FileSlots::fixed::<65_279, 256>();
    assert_eq!(split.capacity(), 65_535);
    assert_eq!(split.accept(), 65_279);
    assert_eq!(split.outbound(), 256);
    assert_eq!(size_of::<settings::FileSlots>(), size_of::<[u32; 2]>());
}

#[test]
fn queue_layout_rejects_invalid_shapes_and_relations() {
    use settings::QueueLayout;

    assert_eq!(QueueLayout::new(0, 8), None);
    assert_eq!(QueueLayout::new(4, 8), None);
    assert_eq!(QueueLayout::new(3, 8), None);
    assert_eq!(QueueLayout::new(65_536, 65_536), None);
    assert_eq!(QueueLayout::new(64, 3), None);
    assert_eq!(QueueLayout::new(64, 32), None);
    assert_eq!(QueueLayout::new(64, 131_072), None);
    assert_eq!(QueueLayout::new(32_768, 65_536), Some(QueueLayout::MAX));
    assert_eq!(size_of::<QueueLayout>(), size_of::<[u32; 2]>());
    assert_eq!(
        settings::Config::default().queue_layout(),
        QueueLayout::fixed::<1024, 2048>()
    );
    assert_eq!(
        settings::Config::for_quic_udp(2, 8)
            .expect("driver config")
            .queue_layout(),
        QueueLayout::fixed::<256, 1024>()
    );
}

#[test]
fn completion_progress_preserves_profile_intent_without_expanding_config() {
    struct Prompt;

    impl Profile for Prompt {
        const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
        const COMPLETION_PROGRESS: settings::CompletionProgress =
            settings::CompletionProgress::Prompt;
    }

    struct Batched;

    impl Profile for Batched {
        const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
    }

    assert_eq!(size_of::<settings::CompletionProgress>(), size_of::<bool>());
    assert_eq!(size_of::<settings::Config>(), size_of::<[u32; 9]>());
    assert_eq!(
        settings::Config::default().completion_progress(),
        settings::CompletionProgress::Prompt
    );
    assert_eq!(
        settings::Config::for_quic_udp(2, 8)
            .expect("UDP config")
            .completion_progress(),
        settings::CompletionProgress::Prompt
    );
    assert_eq!(
        settings::Config::default()
            .with_completion_progress(settings::CompletionProgress::BatchedWhenSupported)
            .completion_progress(),
        settings::CompletionProgress::BatchedWhenSupported
    );
    assert_eq!(
        settings::Config::for_profile::<Prompt>()
            .expect("prompt profile")
            .completion_progress(),
        settings::CompletionProgress::Prompt
    );
    assert_eq!(
        settings::Config::for_tcp_profile::<Batched>(1)
            .expect("batched profile")
            .completion_progress(),
        settings::CompletionProgress::BatchedWhenSupported
    );
}

#[test]
fn scheduler_layout_keeps_correctness_and_cache_capacities_distinct() {
    use settings::{ScheduleCapacity, SchedulerLayout};

    assert_eq!(ScheduleCapacity::ZERO.get(), 0);
    assert_eq!(
        ScheduleCapacity::new(u32::MAX as usize).map(ScheduleCapacity::get),
        Some(u32::MAX as usize)
    );
    if let Some(overflow) = (u32::MAX as usize).checked_add(1) {
        assert_eq!(ScheduleCapacity::new(overflow), None);
    }
    assert_eq!(size_of::<ScheduleCapacity>(), size_of::<u32>());

    let layout = SchedulerLayout::new(7, 11).expect("scheduler layout");
    assert_eq!(layout.ready().get(), 7);
    assert_eq!(layout.timer_cache_limit().get(), 11);
    assert_eq!(size_of::<SchedulerLayout>(), size_of::<[u32; 2]>());

    let changed = layout.with_ready(ScheduleCapacity::fixed::<13>());
    assert_eq!(changed.ready().get(), 13);
    assert_eq!(changed.timer_cache_limit(), layout.timer_cache_limit());

    let changed = layout.with_timer_cache_limit(ScheduleCapacity::ZERO);
    assert_eq!(changed.ready(), layout.ready());
    assert_eq!(changed.timer_cache_limit(), ScheduleCapacity::ZERO);
    assert_eq!(
        settings::Config::for_quic_udp(2, 8)
            .expect("driver config")
            .scheduler(),
        SchedulerLayout::DEFAULT
    );
}

#[test]
fn scheduler_layout_rejects_a_composite_ready_index_overflow_before_allocation() {
    let config = settings::Config::for_quic_udp(2, 8).expect("driver config");
    let scheduler = config
        .scheduler()
        .with_ready(settings::ScheduleCapacity::MAX);
    let error = match Driver::new(config.with_scheduler(scheduler)) {
        Ok(_) => panic!("ready index overflow was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn token_slabs_issue_bounded_keys() {
    type Tag = KeyTag<ROUTE, { kind::SEND }>;

    let capacity = Capacity::new(1).expect("bounded capacity");
    let mut slab = Slab::<_, Tag>::with_capacity(capacity);
    let key = slab.insert(7).expect("slot");
    let token = Token::from_key(key);
    assert_eq!(token.slot(), SlotIndex::ZERO);
    assert_eq!(token.kind(), kind::SEND);
    let parts = token.parts::<Tag>().expect("typed parts");
    assert_eq!(Token::from_parts(parts), token);
    assert_eq!(slab.get(key), Some(&7));

    let cells = CellSlab::<_, Tag>::with_capacity(capacity);
    let key = cells.insert(9).expect("slot");
    assert_eq!(Token::from_key(key).slot(), SlotIndex::ZERO);
    assert_eq!(cells.update(key, |value| *value), Some(9));

    let overflow = SLOT_MASK as usize + 2;
    assert!(Capacity::new(overflow).is_none());
}

#[test]
fn token_slabs_reuse_physical_slots_without_reviving_stale_logical_keys() {
    type Tag = KeyTag<ROUTE>;

    let capacity = Capacity::new(1).expect("bounded capacity");
    let mut slab = Slab::<_, Tag>::with_capacity(capacity);
    let stale = slab.insert(7).expect("first slot");
    assert_eq!(slab.remove(stale), Some(7));
    let current = slab.insert(11).expect("reused slot");
    assert_eq!(current.slot(), stale.slot());
    assert_ne!(current.epoch(), stale.epoch());
    assert_eq!(slab.get(stale), None);
    assert_eq!(slab.get(current), Some(&11));

    let cells = CellSlab::<_, Tag>::with_capacity(capacity);
    let stale = cells.insert(13).expect("first cell slot");
    assert_eq!(cells.remove(stale), Some(13));
    let current = cells.insert(17).expect("reused cell slot");
    assert_eq!(current.slot(), stale.slot());
    assert_ne!(current.epoch(), stale.epoch());
    assert_eq!(cells.update(stale, |_| ()), None);
    assert_eq!(cells.update(current, |value| *value), Some(17));
}

fn outbound_base<const ID: u8>(reservation: &OutboundReservation<'_, ID>) -> u32 {
    reservation
        .physical_index(SlotIndex::ZERO)
        .expect("non-empty reservation")
}

fn nofile_soft_limit() -> libc::rlim_t {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` names writable storage for one resource limit.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    limit.rlim_cur
}

#[test]
fn driver_does_not_raise_nofile() {
    let before = nofile_soft_limit();
    let driver =
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver");
    let after = nofile_soft_limit();
    assert_eq!(after, before);
    drop(driver);
}

#[test]
fn timer_outlives_an_early_scope_drop() {
    let config = with_timer_cache_limit(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        3,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let timer = scope.context().timer();
        fn end(_: driver::lifecycle::Scope<'_>) {}
        end(scope);
        assert_eq!(timer.capacity(), 3);
    });
}

#[test]
fn timer_expiration_consumes_the_active_turn_budget() {
    let config = with_timer_cache_limit(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        2,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |mut context, mut turn| {
            let timer = context.timer();
            let reference = context.driver_ref();
            let slot = reference
                .ready()
                .make_ready_slot(target(reference))
                .expect("ready slot");
            let now = Instant::now();
            let first = pin!(Registration::new(timer));
            let second = pin!(Registration::new(timer));
            first.as_ref().arm(
                reference.scheduler().deadline(now + Duration::from_secs(1)),
                Waker::from_ready(context.driver_ref(), slot.key()),
            );
            second.as_ref().arm(
                reference.scheduler().deadline(now + Duration::from_secs(2)),
                Waker::from_ready(context.driver_ref(), slot.key()),
            );

            let active = turn.begin(1);
            let timers = active.turn().timers();
            timer.expire(timers, &mut context, now);
            assert_eq!(timers.remaining(), 0);
            drop(active);

            let active = turn.begin(1);
            let timers = active.turn().timers();
            timer.expire(timers, &mut context, now);
            assert_eq!(timers.remaining(), 0);
            drop(active);
        });
    });
}

#[test]
fn zero_timer_cache_preserves_deadlines_in_the_intrusive_overflow() {
    let config = with_timer_cache_limit(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        0,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |mut context, mut turn| {
            let timer = context.timer();
            assert_eq!(timer.capacity(), 0);
            let reference = context.driver_ref();
            let expected = target(reference);
            let slot = reference
                .ready()
                .make_ready_slot(expected)
                .expect("ready slot");
            let now = Instant::now();
            let registration = pin!(Registration::new(timer));
            registration.as_ref().arm(
                reference.scheduler().deadline(now + Duration::from_secs(1)),
                Waker::from_ready(context.driver_ref(), slot.key()),
            );

            let active = turn.begin(1);
            timer.expire(
                active.turn().timers(),
                &mut context,
                now + Duration::from_secs(2),
            );
            drop(active);

            let mut active = turn.begin(1);
            let mut fired = Vec::new();
            assert_eq!(active.drain_ready(usize::MAX, |value| fired.push(value)), 1);
            assert_eq!(fired.len(), 1);
            assert!(expected.matches(fired[0]));
            drop(active);
        });
    });
}

#[test]
fn ready_batch_admission_is_fallible_and_atomic() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        2,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let access = scope.context();
        let reference = access.driver_ref();
        let ready = target(reference);
        let held = reference
            .ready()
            .make_ready_slot(ready)
            .expect("first lease");
        let error = match reference
            .ready()
            .make_ready_slots([ready.with_kind(1), ready.with_kind(2)])
        {
            Ok(_) => panic!("oversized ready batch was admitted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        drop(held);
        let batch = reference
            .ready()
            .make_ready_slots([ready.with_kind(1), ready.with_kind(2)])
            .expect("atomic failure must not consume a slot");
        assert_eq!(batch.len(), 2);
    });
}

#[test]
fn dropped_task_admission_releases_its_slot() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        2,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let reference = scope.context().driver_ref();
        let root_slot = reference
            .ready()
            .make_ready_slot(target(reference))
            .expect("root ready slot");
        let root = completion::Wake::from_ready(reference, root_slot.key());
        let first = Box::pin(task::Node::new());
        let second = Box::pin(task::Node::new());

        let admission = task::raw::Binding::admit(root, first.as_ref()).expect("task slot");
        assert!(task::raw::Binding::admit(root, second.as_ref()).is_none());
        drop(admission);
        let replacement =
            task::raw::Binding::admit(root, second.as_ref()).expect("released task slot");
        drop(replacement);
    });
}

#[test]
fn task_domain_reserves_atomically_and_returns_unclaimed_capacity() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        3,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let reference = scope.context().driver_ref();
        let root_slot = reference
            .ready()
            .make_ready_slot(target(reference))
            .expect("root ready slot");
        let root = completion::Wake::from_ready(reference, root_slot.key());

        let error = match task::Domain::<(), 3>::try_new(root) {
            Ok(_) => panic!("oversized task lease was admitted"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            task::Error::Capacity {
                requested: 3,
                available: 2,
            }
        );

        let domain =
            task::Domain::<(), 2>::try_new(root).expect("atomic failure preserved capacity");
        let blocked = Box::pin(task::Node::new());
        assert!(task::raw::Binding::admit(root, blocked.as_ref()).is_none());
        drop(domain);

        let first = Box::pin(task::Node::new());
        let second = Box::pin(task::Node::new());
        let first = task::raw::Binding::admit(root, first.as_ref()).expect("first returned slot");
        let second =
            task::raw::Binding::admit(root, second.as_ref()).expect("second returned slot");
        drop((first, second));
    });
}

#[test]
fn task_domain_restores_a_dropped_admission_and_reclaims_a_binding() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        2,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let reference = scope.context().driver_ref();
        let root_slot = reference
            .ready()
            .make_ready_slot(target(reference))
            .expect("root ready slot");
        let root = completion::Wake::from_ready(reference, root_slot.key());
        let mut domain = task::Domain::<(), 1>::try_new(root).expect("one task credit");
        let node = Box::pin(task::Node::new());
        let ready = Box::pin(set::Set::<usize>::with_capacity(1));
        let unused = domain.admit(node.as_ref()).expect("leased admission");
        drop(unused);
        let admission = domain.admit(node.as_ref()).expect("restored admission");
        // SAFETY: the admitted node and ready set remain pinned through reclaim.
        let wake = unsafe { task::raw::Binding::bind_leased(admission, ready.as_ref(), 0) };
        wake.wake();
        crate::with_turn(&mut scope, |_context, turn| {
            assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
        });
        assert_eq!(ready.as_ref().pop(), Some(0));
        // SAFETY: this node was admitted by `domain` immediately above.
        assert_eq!(
            unsafe { task::raw::Binding::reclaim_domain(&mut domain, node.as_ref()) },
            Some(0)
        );

        let blocked = Box::pin(task::Node::new());
        assert!(task::raw::Binding::admit(root, blocked.as_ref()).is_none());
        drop(domain);
        let returned =
            task::raw::Binding::admit(root, blocked.as_ref()).expect("domain returned its credit");
        drop(returned);
    });
}

#[test]
fn task_domains_interleave_with_global_admission_without_overcommit() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        4,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let reference = scope.context().driver_ref();
        let root_slot = reference
            .ready()
            .make_ready_slot(target(reference))
            .expect("root ready slot");
        let root = completion::Wake::from_ready(reference, root_slot.key());
        let first = task::Domain::<(), 1>::try_new(root).expect("first domain");
        let second = task::Domain::<(), 1>::try_new(root).expect("second domain");
        let global_node = Box::pin(task::Node::new());
        let blocked_node = Box::pin(task::Node::new());
        let global = task::raw::Binding::admit(root, global_node.as_ref())
            .expect("one global credit remains");
        assert!(task::raw::Binding::admit(root, blocked_node.as_ref()).is_none());

        drop(global);
        let restored = task::raw::Binding::admit(root, blocked_node.as_ref())
            .expect("dropped global admission restores only its own credit");
        drop(restored);
        drop(first);
        let first_returned = task::raw::Binding::admit(root, global_node.as_ref())
            .expect("first domain returned one credit");
        drop(first_returned);
        drop(second);
    });
}

#[test]
fn idle_task_domain_retargets_without_releasing_its_reserved_credit() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        3,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let reference = scope.context().driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let first = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("first root slot");
        let second = reference
            .ready()
            .make_ready_slot(
                targets
                    .bind(SlotIndex::try_new(1).expect("second slot"), Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("second root slot");
        let first = completion::Wake::from(first.target());
        let second = completion::Wake::from(second.target());
        let mut domain = task::Domain::<(), 1>::try_new(first).expect("one task domain slot");
        let node = Box::pin(task::Node::new());
        let ready = Box::pin(set::Set::<usize>::with_capacity(1));

        let admission = domain.admit(node.as_ref()).expect("domain admission");
        // SAFETY: node and ready remain pinned until domain reclaim below.
        let _wake = unsafe { task::raw::Binding::bind_leased(admission, ready.as_ref(), 0) };
        assert!(
            !domain.retarget(second),
            "a live binding pins its exact parent"
        );
        // SAFETY: this node was admitted by `domain` immediately above.
        assert_eq!(
            unsafe { task::raw::Binding::reclaim_domain(&mut domain, node.as_ref()) },
            Some(0)
        );
        assert!(
            domain.retarget(second),
            "an idle domain may enter the next root"
        );

        let blocked = Box::pin(task::Node::new());
        assert!(
            task::raw::Binding::admit(second, blocked.as_ref()).is_none(),
            "retargeting keeps the credit unavailable to global admission"
        );
        let admission = domain.admit(node.as_ref()).expect("reused task credit");
        // SAFETY: node and ready remain pinned until domain reclaim below.
        let _wake = unsafe { task::raw::Binding::bind_leased(admission, ready.as_ref(), 0) };
        // SAFETY: this node was admitted by `domain` immediately above.
        assert_eq!(
            unsafe { task::raw::Binding::reclaim_domain(&mut domain, node.as_ref()) },
            Some(0)
        );
    });
}

#[test]
fn task_wake_chain_accepts_the_ceiling_and_rejects_the_next_child_atomically() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        task::MAX_WAKE_HOPS + 2,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let reference = scope.context().driver_ref();
        let root_slot = reference
            .ready()
            .make_ready_slot(target(reference))
            .expect("root ready slot");
        let root = completion::Wake::from_ready(reference, root_slot.key());
        let mut parent = root;
        let mut nodes = Vec::with_capacity(task::MAX_WAKE_HOPS);
        let mut sets = Vec::with_capacity(task::MAX_WAKE_HOPS);

        for _ in 0..task::MAX_WAKE_HOPS {
            let node = Box::pin(task::Node::new());
            let ready = Box::pin(set::Set::<usize>::with_capacity(1));
            let admission =
                task::raw::Binding::admit(parent, node.as_ref()).expect("child within ceiling");
            // SAFETY: child was admitted for this node and both boxed endpoints
            // remain pinned until reverse-order unbind below.
            parent = unsafe { task::raw::Binding::bind(admission, ready.as_ref(), 0) };
            nodes.push(node);
            sets.push(ready);
        }

        let overflow = Box::pin(task::Node::new());
        assert!(task::raw::Binding::admit(parent, overflow.as_ref()).is_none());
        assert!(!task::raw::Binding::is_bound(overflow.as_ref()));
        assert!(task::raw::Binding::waker(overflow.as_ref()).is_none());
        assert!(!task::raw::Binding::wake(overflow.as_ref()));

        parent.wake();
        crate::with_turn(&mut scope, |_context, turn| {
            for _ in 0..task::MAX_WAKE_HOPS {
                turn.drain_ready(usize::MAX, drop);
            }
        });
        assert!(sets.iter().all(|ready| ready.as_ref().pop() == Some(0)));

        // Stale generation keys remain safe while the chain is dismantled.
        parent = root;
        assert!(parent == root);
        for node in nodes.iter().rev() {
            assert_eq!(task::raw::Binding::unbind(node.as_ref()), Some(0));
        }
        assert!(sets.iter().all(|ready| ready.as_ref().is_empty()));
    });
}

#[test]
fn stale_task_wake_cannot_reach_a_dropped_or_reused_node() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        2,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let reference = scope.context().driver_ref();
        let root_slot = reference
            .ready()
            .make_ready_slot(target(reference))
            .expect("root ready slot");
        let root = completion::Wake::from_ready(reference, root_slot.key());

        let stale = {
            let node = Box::pin(task::Node::new());
            let ready = Box::pin(set::Set::<usize>::with_capacity(1));
            let admission =
                task::raw::Binding::admit(root, node.as_ref()).expect("first task slot");
            // SAFETY: both boxed endpoints stay pinned through unbind.
            let wake = unsafe { task::raw::Binding::bind(admission, ready.as_ref(), 0) };
            assert_eq!(task::raw::Binding::unbind(node.as_ref()), Some(0));
            wake
        };

        let replacement = Box::pin(task::Node::new());
        let replacement_ready = Box::pin(set::Set::<usize>::with_capacity(1));
        let admission = task::raw::Binding::admit(root, replacement.as_ref())
            .expect("generation-reused task slot");
        // SAFETY: both boxed endpoints stay pinned through unbind.
        let replacement_wake =
            unsafe { task::raw::Binding::bind(admission, replacement_ready.as_ref(), 0) };

        stale.wake();
        crate::with_turn(&mut scope, |_context, turn| {
            assert_eq!(turn.drain_ready(usize::MAX, drop), 0);
        });
        assert!(replacement_ready.is_empty());

        replacement_wake.wake();
        crate::with_turn(&mut scope, |_context, turn| {
            assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
        });
        assert_eq!(replacement_ready.as_ref().pop(), Some(0));
        assert_eq!(task::raw::Binding::unbind(replacement.as_ref()), Some(0));
    });
}

#[test]
fn unbinding_a_queued_child_promotes_its_wake_to_the_live_parent() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        2,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let reference = scope.context().driver_ref();
        let expected = target(reference);
        let root_slot = reference
            .ready()
            .make_ready_slot(expected)
            .expect("root ready slot");
        let root = completion::Wake::from_ready(reference, root_slot.key());
        let node = Box::pin(task::Node::new());
        let ready = Box::pin(set::Set::<usize>::with_capacity(1));
        let admission = task::raw::Binding::admit(root, node.as_ref()).expect("task slot");
        // SAFETY: both boxed endpoints remain pinned through unbind below.
        let wake = unsafe { task::raw::Binding::bind(admission, ready.as_ref(), 0) };

        wake.wake();
        assert_eq!(task::raw::Binding::unbind(node.as_ref()), Some(0));
        assert!(ready.is_empty());

        let mut activated = Vec::new();
        crate::with_turn(&mut scope, |_context, turn| {
            assert_eq!(
                turn.drain_ready(usize::MAX, |value| activated.push(value)),
                1
            );
        });
        assert_eq!(activated.len(), 1);
        assert!(expected.matches(activated[0]));
    });
}

#[test]
fn bounded_ready_drain_preserves_the_remaining_snapshot() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        3,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |context, turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let targets = [ready.with_kind(1), ready.with_kind(2), ready.with_kind(3)];
            let slots = reference
                .ready()
                .make_ready_slots(targets)
                .expect("ready slots");
            for slot in &slots {
                slot.activate();
            }

            let mut first = Vec::new();
            let drained = turn.drain_ready(2, |token| first.push(token));
            assert_eq!(drained, 2);
            assert_eq!(first.len(), 2);
            assert!(targets[0].matches(first[0]));
            assert!(targets[1].matches(first[1]));
            assert!(reference.ready().has_ready());

            let mut remaining = Vec::new();
            turn.drain_ready(usize::MAX, |token| remaining.push(token));
            assert_eq!(remaining.len(), 1);
            assert!(targets[2].matches(remaining[0]));
            assert!(!reference.ready().has_ready());
        });
    });
}

#[test]
fn bounded_ready_drain_resumes_snapshot_before_pending_batch() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        4,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |context, turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let targets = [
                ready.with_kind(1),
                ready.with_kind(2),
                ready.with_kind(3),
                ready.with_kind(4),
            ];
            let slots = reference
                .ready()
                .make_ready_slots(targets)
                .expect("ready slots");
            for slot in &slots[..3] {
                slot.activate();
            }

            let mut first = Vec::new();
            turn.drain_ready(1, |token| {
                first.push(token);
                slots[3].activate();
            });
            assert_eq!(first.len(), 1);
            assert!(targets[0].matches(first[0]));

            let mut snapshot = Vec::new();
            turn.drain_ready(usize::MAX, |token| snapshot.push(token));
            assert_eq!(snapshot.len(), 2);
            assert!(targets[1].matches(snapshot[0]));
            assert!(targets[2].matches(snapshot[1]));

            let mut pending = Vec::new();
            turn.drain_ready(usize::MAX, |token| pending.push(token));
            assert_eq!(pending.len(), 1);
            assert!(targets[3].matches(pending[0]));
        });
    });
}

#[test]
fn ready_controller_cannot_exceed_or_refill_the_turn_budget() {
    let capacity = schedule::MAX_TURN_WORK_BUDGET + 44;
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        capacity,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |context, mut turn| {
            let reference = context.driver_ref();
            let slots = reference
                .ready()
                .make_ready_slots(vec![target(reference); capacity])
                .expect("ready slots");
            for slot in &slots {
                slot.activate();
            }

            let mut active = turn.begin(usize::MAX);
            let mut activated = 0;
            let drained = active.drain_ready(usize::MAX, |_| activated += 1);
            assert_eq!(drained, schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(activated, schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(active.drain_ready(usize::MAX, drop), 0);
            assert!(reference.ready().has_ready());
            drop(active);
        });
    });
}

#[test]
fn ready_controller_charges_work_before_calling_user_code() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        2,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |context, mut turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let slots = reference
                .ready()
                .make_ready_slots([ready.with_kind(1), ready.with_kind(2)])
                .expect("ready slots");
            for slot in &slots {
                slot.activate();
            }

            let mut active = turn.begin(1);
            Expectation::any().assert(|| {
                active.drain_ready(usize::MAX, |_| panic!("activation failed"));
            });
            assert_eq!(active.drain_ready(usize::MAX, drop), 0);
            assert!(reference.ready().has_ready());
            drop(active);
        });
    });
}

#[test]
fn application_reservation_is_atomic_and_returns_unused_work() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |context, mut turn| {
            let active = turn.begin(4);
            let mut context = context;
            assert!(
                reservation::Application::reserve(active.turn().application(), &mut context, 5,)
                    .is_none()
            );
            let reservation =
                reservation::Application::reserve(active.turn().application(), &mut context, 3)
                    .expect("three units");
            let work = active.turn().application();
            assert!(work.take(), "only unreserved work remains available");
            assert!(
                !work.take(),
                "reserved work cannot be spent through an alias"
            );
            let context = reservation.commit(1);
            let work = active.turn().application();
            assert!(work.take());
            assert!(work.take());
            assert!(!work.take());
            drop(active);

            let active = turn.begin(2);
            {
                let _reservation =
                    reservation::Application::reserve(active.turn().application(), context, 2)
                        .expect("two units");
                assert!(!active.turn().application().take());
            }
            let work = active.turn().application();
            assert!(work.take());
            assert!(work.take());
            assert!(!work.take());
            drop(active);
        });
    });
}

#[test]
fn maintenance_permit_is_zero_sized_and_charges_exactly_once() {
    assert_eq!(
        std::mem::size_of::<driver::schedule::MaintenancePermit<'static, 'static>>(),
        0
    );
    assert_eq!(
        std::mem::size_of::<driver::schedule::Turn<'static, 'static>>(),
        std::mem::size_of::<usize>()
    );
    assert_eq!(
        std::mem::size_of::<driver::schedule::Application<'static, 'static>>(),
        std::mem::size_of::<usize>()
    );
    assert_eq!(
        std::mem::size_of::<driver::schedule::Maintenance<'static, 'static>>(),
        std::mem::size_of::<usize>()
    );
    assert_eq!(
        std::mem::size_of::<driver::schedule::Half<'static, 'static>>(),
        std::mem::size_of::<usize>()
    );
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |_context, mut turn| {
            let active = turn.begin(2);

            let permit = driver::schedule::MaintenancePermit::try_take(active.turn().maintenance())
                .expect("first maintenance transition");
            drop(permit);
            assert_eq!(active.turn().maintenance().remaining(), 1);

            let permit = driver::schedule::MaintenancePermit::try_take(active.turn().maintenance())
                .expect("second maintenance transition");
            drop(permit);
            assert_eq!(active.turn().maintenance().remaining(), 0);
            assert!(
                driver::schedule::MaintenancePermit::try_take(active.turn().maintenance())
                    .is_none()
            );
            drop(active);
        });
    });
}

#[test]
fn maintenance_permit_splits_the_matching_region_under_one_turn_borrow() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |context, mut turn| {
            let active = turn.begin(1);
            let mut context = context;
            let (permit, region) = driver::schedule::MaintenancePermit::try_take_with_region(
                active.turn().maintenance(),
                &mut context,
            )
            .expect("maintenance transition and matching region");
            let _ = region;
            drop(permit);
            assert_eq!(active.turn().maintenance().remaining(), 0);
            drop(active);
        });
    });
}

#[test]
fn maintenance_share_is_const_bounded_and_nested() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |_context, mut turn| {
            let active = turn.begin(10);
            let work = active.turn().maintenance();

            let outer = work.share::<3>();
            assert_eq!(work.remaining(), 4);
            assert!(work.take());

            let inner = work.share::<2>();
            assert_eq!(work.remaining(), 2);
            assert!(work.take());
            drop(inner);
            assert_eq!(work.remaining(), 2);
            drop(outer);
            assert_eq!(work.remaining(), 8);

            let maximum = work.share::<{ usize::MAX }>();
            assert_eq!(work.remaining(), 1);
            assert!(work.take());
            drop(maximum);
            assert_eq!(work.remaining(), 7);

            let single = work.share::<1>();
            assert_eq!(work.remaining(), 7);
            assert!(work.take());
            drop(single);
            assert_eq!(work.remaining(), 6);
            drop(active);
        });
    });
}

#[test]
fn maintenance_half_is_persistent_within_a_turn_and_resets_next_turn() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |_context, mut turn| {
            let active = turn.begin(5);

            {
                let first = active.turn().maintenance().half();
                assert_eq!(first.remaining(), 3);
                assert!(first.take());
                assert!(first.take());
            }

            let reentered = active.turn().maintenance().half();
            assert_eq!(reentered.remaining(), 1);
            assert!(reentered.take());
            assert!(!reentered.take());
            assert_eq!(active.turn().maintenance().remaining(), 2);
            drop(active);

            let active = turn.begin(5);
            let next_turn = active.turn().maintenance().half();
            assert_eq!(next_turn.remaining(), 3);
            assert!(next_turn.take());
            assert_eq!(active.turn().maintenance().remaining(), 4);
            drop(active);
        });
    });
}

#[test]
fn wake_of_an_unpolled_ready_slot_is_coalesced() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |context, turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let targets = [ready.with_kind(1), ready.with_kind(2)];
            let slots = reference
                .ready()
                .make_ready_slots(targets)
                .expect("ready slots");
            let first = slots.first().expect("first ready slot");
            let second = slots.get(1).expect("second ready slot");
            first.activate();
            second.activate();

            let mut drained = Vec::new();
            turn.drain_ready(usize::MAX, |token| {
                drained.push(token);
                if targets[0].matches(token) {
                    second.activate();
                }
            });

            assert_eq!(drained.len(), 2);
            assert!(targets[0].matches(drained[0]));
            assert!(targets[1].matches(drained[1]));
            assert!(!reference.ready().has_ready());
        });
    });
}

#[test]
fn wake_after_dequeue_is_deferred_to_the_next_batch() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |context, turn| {
            let reference = context.driver_ref();
            let target = target(reference);
            let slot = reference
                .ready()
                .make_ready_slot(target)
                .expect("ready slot");
            slot.activate();

            let mut first = Vec::new();
            turn.drain_ready(usize::MAX, |ready| {
                first.push(ready);
                slot.activate();
            });
            let mut second = Vec::new();
            turn.drain_ready(usize::MAX, |ready| second.push(ready));

            assert_eq!(first.len(), 1);
            assert!(target.matches(first[0]));
            assert_eq!(second.len(), 1);
            assert!(target.matches(second[0]));
        });
    });
}

#[test]
fn nested_ready_drain_cannot_bypass_the_turn_budget() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_controller(&mut scope, |context, mut turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let targets = [ready.with_kind(1), ready.with_kind(2), ready.with_kind(3)];
            let slots = reference
                .ready()
                .make_ready_slots(targets)
                .expect("ready slots");
            slots[0].activate();
            slots[1].activate();

            let active = turn.begin(2);
            let mut outer = Vec::new();
            let mut nested = Vec::new();
            let drained = active.turn().drain_ready(reference, usize::MAX, |ready| {
                outer.push(ready);
                if outer.len() == 1 {
                    slots[2].activate();
                    active
                        .turn()
                        .drain_ready(reference, usize::MAX, |ready| nested.push(ready));
                }
            });
            assert_eq!(drained, 2);
            assert!(nested.is_empty());
            assert!(reference.ready().has_ready());
            assert_eq!(active.turn().drain_ready(reference, usize::MAX, drop), 0);
            drop(active);

            let active = turn.begin(1);
            let mut deferred = Vec::new();
            assert_eq!(
                active
                    .turn()
                    .drain_ready(reference, usize::MAX, |ready| deferred.push(ready)),
                1
            );
            assert_eq!(deferred.len(), 1);
            assert!(targets[2].matches(deferred[0]));
            drop(active);
        });
    });
}

#[test]
fn dropping_pending_snapshot_slot_unlinks_it() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |context, turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let targets = [ready.with_kind(1), ready.with_kind(2), ready.with_kind(3)];
            let first = reference
                .ready()
                .make_ready_slot(targets[0])
                .expect("first slot");
            let mut second = Some(
                reference
                    .ready()
                    .make_ready_slot(targets[1])
                    .expect("second slot"),
            );
            let third = reference
                .ready()
                .make_ready_slot(targets[2])
                .expect("third slot");
            first.activate();
            second.as_ref().expect("second slot").activate();
            third.activate();
            let mut drained = Vec::new();
            turn.drain_ready(usize::MAX, |token| {
                drained.push(token);
                if targets[0].matches(token) {
                    drop(second.take());
                }
            });
            assert_eq!(drained.len(), 2);
            assert!(targets[0].matches(drained[0]));
            assert!(targets[2].matches(drained[1]));
        });
    });
}

#[test]
fn application_wake_does_not_release_held_receive_credit() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        1,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |mut context, turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut context,
                "127.0.0.1:0".parse().expect("socket address"),
            )
            .expect("fixed ready slot");
            let slot = socket.ready_handle();
            slot.set_target(ready);

            assert!(slot.arm_recv_credit(ready));
            slot.activate();
            let mut activated = Vec::new();
            turn.drain_ready(usize::MAX, |token| activated.push(token));
            assert_eq!(activated.len(), 1);
            assert!(
                ready
                    .with_kind(kind::RECV_CREDIT_HELD)
                    .matches(activated[0])
            );
            assert_eq!(slot.take_recv_credit(ready), None);

            slot.wake_recv_credit(ready, driver::RecvCreditWake::ResourceReturned);
            activated.clear();
            turn.drain_ready(usize::MAX, |token| activated.push(token));
            assert_eq!(activated.len(), 1);
            assert!(
                ready
                    .with_kind(driver::RecvCreditWake::ResourceReturned as u8)
                    .matches(activated[0])
            );
            assert_eq!(
                slot.take_recv_credit(ready),
                Some(driver::RecvCreditWake::ResourceReturned)
            );

            slot.activate();
            activated.clear();
            turn.drain_ready(usize::MAX, |token| activated.push(token));
            assert_eq!(activated.len(), 1);
            assert!(ready.matches(activated[0]));
            ops::Files::close(&mut context, socket);
        });
    });
}

#[test]
fn receive_credit_wake_preserves_waiter_retry_cause() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        1,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |mut context, turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut context,
                "127.0.0.1:0".parse().expect("socket address"),
            )
            .expect("fixed ready slot");
            let slot = socket.ready_handle();
            slot.set_target(ready);

            assert!(slot.arm_recv_credit(ready));
            slot.wake_recv_credit(ready, driver::RecvCreditWake::WaiterRetry);
            let mut activated = Vec::new();
            turn.drain_ready(usize::MAX, |token| activated.push(token));
            assert_eq!(activated.len(), 1);
            assert!(
                ready
                    .with_kind(driver::RecvCreditWake::WaiterRetry as u8)
                    .matches(activated[0])
            );
            assert_eq!(
                slot.take_recv_credit(ready),
                Some(driver::RecvCreditWake::WaiterRetry)
            );

            slot.activate();
            activated.clear();
            turn.drain_ready(usize::MAX, |token| activated.push(token));
            assert_eq!(activated.len(), 1);
            assert!(ready.matches(activated[0]));
            ops::Files::close(&mut context, socket);
        });
    });
}

#[test]
fn cancelled_receive_credit_restores_the_exact_target_without_waking() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        1,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |mut context, turn| {
            let reference = context.driver_ref();
            let ready = target(reference);
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut context,
                "127.0.0.1:0".parse().expect("socket address"),
            )
            .expect("fixed ready slot");
            let slot = socket.ready_handle();
            slot.set_target(ready);

            assert!(slot.arm_recv_credit(ready));
            assert!(!slot.arm_recv_credit(ready));
            assert!(slot.has_recv_credit(ready));
            assert!(slot.cancel_recv_credit(ready));
            assert!(!slot.has_recv_credit(ready));
            assert!(!reference.ready().has_ready());

            slot.activate();
            let mut activated = Vec::new();
            turn.drain_ready(usize::MAX, |token| activated.push(token));
            assert_eq!(activated.len(), 1);
            assert!(ready.matches(activated[0]));
            ops::Files::close(&mut context, socket);
        });
    });
}

#[test]
fn stale_guard_cannot_release_credit_for_a_reused_connection() {
    let config = with_ready_capacity(
        settings::Config::for_quic_udp(2, 8).expect("driver config"),
        1,
    );
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |mut context, turn| {
            let reference = context.driver_ref();
            let old = target(reference);
            let new = reference
                .targets::<KeyTag<ROUTE>>()
                .bind_parts(Parts::from_components(
                    SlotIndex::ZERO,
                    Epoch::INITIAL.next().unwrap(),
                ))
                .dispatch();
            let (socket, _) = ops::Bootstrap::bind_datagram_slot(
                &mut context,
                "127.0.0.1:0".parse().expect("socket address"),
            )
            .expect("fixed ready slot");
            let slot = socket.ready_handle();
            slot.set_target(old);
            assert!(slot.arm_recv_credit(old));

            slot.set_target(new);
            slot.wake_recv_credit(old, driver::RecvCreditWake::ResourceReturned);
            slot.wake_recv_credit(old, driver::RecvCreditWake::WaiterRetry);
            assert!(!reference.ready().has_ready());

            slot.activate();
            let mut activated = Vec::new();
            turn.drain_ready(usize::MAX, |token| activated.push(token));
            assert_eq!(activated.len(), 1);
            assert!(new.matches(activated[0]));
            ops::Files::close(&mut context, socket);
        });
    });
}

#[test]
fn outbound_reservation_rejects_an_empty_slot_set() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        let error = ops::Files::reserve_outbound::<ROUTE>(&mut access, 0)
            .err()
            .expect("empty outbound reservation must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    });
}

#[test]
fn outbound_reservation_only_issues_its_owned_slots() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        let reservation =
            ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).expect("outbound reservation");
        let reference = access.driver_ref();
        assert!(
            reservation
                .bind(tuning_target::<ROUTE>(reference, SlotIndex::ZERO))
                .is_some()
        );
        assert!(
            reservation
                .bind(tuning_target::<ROUTE>(reference, SlotIndex::from(1_u16),))
                .is_none()
        );
    });
}

#[test]
fn unused_outbound_reservation_is_reclaimed_by_a_reactor_turn() {
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<2, 1>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |mut access, turn| {
            let first =
                ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).expect("first reservation");
            drop(first);
            assert_eq!(
                ops::poll::Poll::commit(&mut access, turn.reactor())
                    .expect("reclaim dropped reservation"),
                ops::poll::Commit::Drained
            );

            let second = ops::Files::reserve_outbound::<ROUTE>(&mut access, 1)
                .expect("reactor turn must reclaim the dropped reservation");
            ops::Files::retire_outbound(&mut access, second);
        });
    });
}

#[test]
fn outbound_slots_wait_for_their_last_bound_descriptor() {
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<2, 1>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        crate::with_turn(&mut scope, |mut access, turn| {
            let reservation =
                ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).expect("first reservation");
            let owner = tuning_target::<ROUTE>(access.driver_ref(), SlotIndex::ZERO);
            let descriptor = reservation.bind(owner).expect("bound descriptor");
            drop(reservation);

            assert!(
                ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).is_err(),
                "the slots must remain reserved while their descriptor is live"
            );
            drop(descriptor);
            assert_eq!(
                ops::poll::Poll::commit(&mut access, turn.reactor())
                    .expect("reclaim dropped descriptor slots"),
                ops::poll::Commit::Drained
            );

            let reclaimed = ops::Files::reserve_outbound::<ROUTE>(&mut access, 1)
                .expect("reactor turn must reclaim the last descriptor's slots");
            ops::Files::retire_outbound(&mut access, reclaimed);
        });
    });
}

#[test]
fn unsubmitted_outbound_slots_release_without_reactor_work() {
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<2, 300>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        let reservation =
            ops::Files::reserve_outbound::<ROUTE>(&mut access, 300).expect("outbound reservation");
        for local in 0_u16..300 {
            let owner = tuning_target::<ROUTE>(access.driver_ref(), SlotIndex::from(local));
            let slot = reservation.bind(owner).expect("bind outbound slot");
            drop(slot);
        }
        let last = reservation
            .bind(tuning_target::<ROUTE>(
                access.driver_ref(),
                SlotIndex::from(299_u16),
            ))
            .expect("unsubmitted slot must release synchronously");
        drop(last);
        ops::Files::retire_outbound(&mut access, reservation);
    });
}

#[test]
fn outbound_slot_has_only_one_live_authority() {
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<2, 1>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        let reservation =
            ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).expect("outbound reservation");
        let owner = tuning_target::<ROUTE>(access.driver_ref(), SlotIndex::ZERO);
        let first = reservation.bind(owner).expect("first slot authority");
        assert!(reservation.bind(owner).is_none());
        drop(first);
        let second = reservation
            .bind(owner)
            .expect("slot must be reusable after authority release");
        drop(second);
        ops::Files::retire_outbound(&mut access, reservation);
    });
}

#[test]
fn outbound_route_has_only_one_live_domain() {
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<2, 2>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        let first = ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).expect("first domain");
        assert!(
            ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).is_err(),
            "one route cannot own two outbound domains"
        );
        ops::Files::retire_outbound(&mut access, first);
        let reused = ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).expect("reused domain");
        ops::Files::retire_outbound(&mut access, reused);
    });
}

#[test]
fn outbound_slots_reuse_released_groups() {
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<2, 8>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        let first = ops::Files::reserve_outbound::<ROUTE>(&mut access, 3).expect("first");
        let second = ops::Files::reserve_outbound::<{ ROUTE + 1 }>(&mut access, 2).expect("second");
        let third = ops::Files::reserve_outbound::<{ ROUTE + 2 }>(&mut access, 1).expect("third");
        assert_eq!(
            (
                outbound_base(&first),
                outbound_base(&second),
                outbound_base(&third),
            ),
            (2, 5, 7)
        );

        ops::Files::retire_outbound(&mut access, second);
        let high = ops::Files::reserve_outbound::<{ ROUTE + 1 }>(&mut access, 1).expect("high");
        let low = ops::Files::reserve_outbound::<{ ROUTE + 3 }>(&mut access, 1).expect("low");
        assert_eq!((outbound_base(&high), outbound_base(&low)), (6, 5));
    });
}

#[test]
fn outbound_slots_reuse_in_constant_time() {
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<2, 10>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        let highest = ops::Files::reserve_outbound::<ROUTE>(&mut access, 2).expect("highest");
        let upper_live =
            ops::Files::reserve_outbound::<{ ROUTE + 1 }>(&mut access, 2).expect("upper live");
        let lowest = ops::Files::reserve_outbound::<{ ROUTE + 2 }>(&mut access, 2).expect("lowest");
        let lower_live =
            ops::Files::reserve_outbound::<{ ROUTE + 3 }>(&mut access, 2).expect("lower live");
        assert_eq!(
            (
                outbound_base(&highest),
                outbound_base(&upper_live),
                outbound_base(&lowest),
                outbound_base(&lower_live),
            ),
            (2, 4, 6, 8)
        );

        ops::Files::retire_outbound(&mut access, highest);
        ops::Files::retire_outbound(&mut access, lowest);
        let first = ops::Files::reserve_outbound::<ROUTE>(&mut access, 1).expect("first singleton");
        let second = ops::Files::reserve_outbound::<{ ROUTE + 2 }>(&mut access, 1)
            .expect("second singleton");
        let reclaimed_high =
            ops::Files::reserve_outbound::<{ ROUTE + 4 }>(&mut access, 2).expect("high hole");
        assert_eq!(
            (
                outbound_base(&first),
                outbound_base(&second),
                outbound_base(&reclaimed_high),
            ),
            (7, 6, 3)
        );
    });
}

#[test]
fn outbound_reservation_collects_all_noncontiguous_free_slots() {
    let config = settings::Config::for_quic_udp(2, 8)
        .expect("driver config")
        .with_file_slots(settings::FileSlots::fixed::<2, 8>());
    let mut driver = pin!(Driver::new(config).expect("driver"));
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        let first = ops::Files::reserve_outbound::<ROUTE>(&mut access, 3).expect("first");
        let second = ops::Files::reserve_outbound::<{ ROUTE + 1 }>(&mut access, 2).expect("second");
        let third = ops::Files::reserve_outbound::<{ ROUTE + 2 }>(&mut access, 1).expect("third");
        ops::Files::retire_outbound(&mut access, first);
        ops::Files::retire_outbound(&mut access, second);
        ops::Files::retire_outbound(&mut access, third);

        let reclaimed =
            ops::Files::reserve_outbound::<ROUTE>(&mut access, 8).expect("fully reclaimed");
        let mut physical = (0_u16..8)
            .map(|local| {
                reclaimed
                    .physical_index(SlotIndex::from(local))
                    .expect("mapped physical slot")
            })
            .collect::<Vec<_>>();
        physical.sort_unstable();
        assert_eq!(physical, (2_u32..10).collect::<Vec<_>>());
    });
}

#[test]
fn route_transaction_rolls_back_until_committed() {
    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 8).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        let mut access = scope.context();
        drop(Route::<ROUTE>::reserve_transaction(&mut access).expect("transaction"));

        let route = Route::<ROUTE>::reserve_transaction(&mut access)
            .expect("rolled-back route")
            .commit();
        let error = match Route::<ROUTE>::reserve_transaction(&mut access) {
            Ok(_) => panic!("committed route was released"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);

        route.release(&mut access);
        let route = Route::<ROUTE>::reserve_transaction(&mut access)
            .expect("released route")
            .commit();
        route.release(&mut access);
    });
}

#[test]
fn tcp_profile_explicitly_caps_oversized_connection_counts() {
    struct TestProfile;

    impl Profile for TestProfile {
        const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
        const MAX_ACCEPT_SLOTS: u32 = 112;
        const OUTBOUND_SLOTS: u32 = 16;
    }

    let Ok(oversized) = usize::try_from(u64::from(u32::MAX) + 1) else {
        return;
    };
    let config =
        settings::Config::for_tcp_profile::<TestProfile>(oversized).expect("driver config");
    assert_eq!(config.file_slots().accept(), TestProfile::MAX_ACCEPT_SLOTS);
    assert_eq!(config.file_slots().outbound(), TestProfile::OUTBOUND_SLOTS);
    assert_eq!(
        config.file_slots().capacity(),
        TestProfile::MAX_ACCEPT_SLOTS + TestProfile::OUTBOUND_SLOTS
    );
}

#[test]
fn tcp_profile_preserves_its_receive_layout() {
    struct TestProfile;

    impl Profile for TestProfile {
        const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
        const MAX_ACCEPT_SLOTS: u32 = 112;
        const OUTBOUND_SLOTS: u32 = 16;
        const RECEIVE: settings::Receive = settings::Receive::fixed::<4096, 8192>();
    }

    for connections in [0, 1, 112, usize::MAX] {
        let config =
            settings::Config::for_tcp_profile::<TestProfile>(connections).expect("driver config");
        assert_eq!(config.receive(), TestProfile::RECEIVE);
    }
}

#[test]
fn receive_layout_rejects_invalid_kernel_shapes_at_construction() {
    use settings::Receive;

    assert_eq!(Receive::new(0, 4096), None);
    assert_eq!(Receive::new(1, 4096), None);
    assert_eq!(Receive::new(3, 4096), None);
    assert_eq!(Receive::new(1024, 0), None);
    assert_eq!(
        Receive::new(2, 1)
            .expect("minimum stream slot")
            .buffer_len(),
        1
    );
    assert!(Receive::new(2, transfer::MAX_BYTES as u32).is_some());
    assert_eq!(Receive::new(2, transfer::MAX_BYTES as u32 + 1), None);
    assert_eq!(
        Receive::for_datagram_payload(1, transfer::MAX_BYTES as u32),
        None
    );
    assert!(settings::Config::for_quic_udp(1, 8).is_err());
    assert!(settings::Config::for_quic_udp(3, 4096).is_err());
    assert!(settings::Config::for_quic_udp(1024, 0).is_err());
    assert!(settings::Config::for_quic_udp(u32::from(u16::MAX) + 1, 4096).is_err());
    let udp = settings::Config::for_quic_udp(2, 8).expect("minimum UDP payload layout");
    assert_eq!(
        udp.receive().buffer_len(),
        (Receive::MIN_DATAGRAM_BUFFER_LEN + 7) as usize
    );
}

#[test]
fn stream_sized_receive_pool_cannot_issue_a_datagram_descriptor() {
    struct StreamOnly;

    impl Profile for StreamOnly {
        const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 128>();
        const RECEIVE: settings::Receive = settings::Receive::fixed::<2, 1>();
    }

    Runtime::for_profile::<StreamOnly>().with_driver(|mut driver| {
        let error = ops::Bootstrap::bind_datagram_slot(
            &mut driver,
            "127.0.0.1:0".parse().expect("datagram address"),
        )
        .expect_err("stream-sized receive storage cannot prove datagram capacity");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    });
}
