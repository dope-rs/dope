use dope_core::driver::{life::recovery, route::KeyTag, schedule::ready::task::topology};

use crate::common::{
    driver::{
        Scoped as _,
        support::{Configure as _, Targets as _},
    },
    wake::Ready as _,
};

pub mod common;

use dope_core::driver::{
    route::{Epoch, indices::SlotIndex},
    schedule,
    schedule::ready::task,
    settings,
};

use crate::common::{
    driver::{support::ROUTE, task::Index},
    rt::{Runtime, finish, recover},
};

#[test]
fn wake_ceiling() {
    let config = (Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE))
    .with_ready(task::MAX_WAKE_HOPS * 2 + 1)
    .expect("ready capacity");
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            let dref = scope.context().driver_ref();
            let slot = (dref.ready().make_slot(dref.target())).expect("root ready slot");
            let root = task::Parent::from(slot.target());
            let mut parent = root;
            let mut nodes = Vec::with_capacity(task::MAX_WAKE_HOPS);
            let mut sets = Vec::with_capacity(task::MAX_WAKE_HOPS);

            for _ in 0..task::MAX_WAKE_HOPS {
                let ready = Box::pin(topology::Slot::new());
                let admit = (parent.try_admit()).expect("child within ceiling");
                // SAFETY: each edge belongs to the admitted parent, and its endpoint
                // remains pinned until reverse-order root release below.
                let binding = unsafe {
                    task::raw::Root::bind(
                        admit,
                        ready
                            .try_attach(parent, Index(0))
                            .expect("task topology fixture"),
                    )
                };
                parent = binding.poll().1;
                nodes.push(binding);
                sets.push(ready);
            }

            assert!(matches!(
                parent.try_admit(),
                Err(task::Error::WakeHopCeiling {
                    minimum: task::MAX_WAKE_HOPS,
                    maximum,
                }) if maximum == task::MAX_WAKE_HOPS - 1
            ));

            parent.wake();
            scope.run_turn(|_context, turn| {
                for _ in 0..task::MAX_WAKE_HOPS {
                    turn.ready_round(|_| {});
                }
            });
            assert!(sets.iter().all(|ready| ready.pop() == Some(Index(0))));

            while nodes.pop().is_some() {
                parent.wake();
            }
            assert!(sets.iter().all(|ready| ready.is_empty()));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
#[test]
fn stale_wake() {
    let config = (Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE))
    .with_ready(3)
    .expect("ready capacity");
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            let dref = scope.context().driver_ref();
            let slot = (dref.ready().make_slot(dref.target())).expect("root ready slot");
            let root = task::Parent::from(slot.target());

            let stale = {
                let ready = Box::pin(topology::Slot::new());
                let admit = (root.try_admit()).expect("first task slot");
                // SAFETY: the matching endpoint stays pinned through root release.
                let binding = unsafe {
                    task::raw::Root::bind(
                        admit,
                        ready
                            .try_attach(root, Index(0))
                            .expect("task topology fixture"),
                    )
                };
                binding.poll().1
            };

            let set = Box::pin(topology::Slot::new());
            let admit = (root.try_admit()).expect("generation-reused task slot");
            // SAFETY: the matching endpoint stays pinned through root release.
            let binding = unsafe {
                task::raw::Root::bind(
                    admit,
                    set.try_attach(root, Index(0))
                        .expect("task topology fixture"),
                )
            };
            let wake = binding.poll().1;

            stale.wake();
            scope.run_turn(|_context, turn| {
                assert_eq!(turn.ready_round(|_| {}), 0);
            });
            assert!(set.is_empty());

            wake.wake();
            scope.run_turn(|_context, turn| {
                assert_eq!(turn.ready_round(|_| {}), 1);
            });
            assert_eq!(set.pop(), Some(Index(0)));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn drop_reclaims() {
    let config = (Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE))
    .with_ready(3)
    .expect("ready capacity");
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            let dref = scope.context().driver_ref();
            let slot = (dref.ready().make_slot(dref.target())).expect("root ready slot");
            let root = task::Parent::from(slot.target());
            {
                let binding = {
                    let ready = Box::pin(topology::Slot::new());
                    let admit = (root.try_admit()).expect("task slot");
                    // SAFETY: the admitted root and edge belong to this driver. Dropping
                    // the endpoint defers reclamation until the root binding is released.
                    unsafe {
                        task::raw::Root::bind(
                            admit,
                            ready
                                .try_attach(root, Index(0))
                                .expect("task topology fixture"),
                        )
                    }
                };
                assert!(binding.poll().1.belongs_to(dref));
            }

            let targets = [dref.target(), dref.target()];
            let slots = (targets
                .iter()
                .copied()
                .map(|target| dref.ready().make_slot(target))
                .collect::<Result<Vec<_>, _>>()
                .map(Vec::into_boxed_slice))
            .expect("task and deferred topology slots reclaimed");
            assert_eq!(slots.len(), targets.len());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn unbind_promotes() {
    let config = (Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE))
    .with_ready(3)
    .expect("ready capacity");
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            let dref = scope.context().driver_ref();
            let expected = dref.target();
            let slot = (dref.ready().make_slot(expected)).expect("root ready slot");
            let root = task::Parent::from(slot.target());
            let ready = Box::pin(topology::Slot::new());
            let admit = (root.try_admit()).expect("task slot");
            {
                // SAFETY: the matching endpoint remains pinned through root release.
                let binding = unsafe {
                    task::raw::Root::bind(
                        admit,
                        ready
                            .try_attach(root, Index(0))
                            .expect("task topology fixture"),
                    )
                };
                let wake = binding.poll().1;

                wake.wake();
                let mut drain = ready.drain().expect("ready drain");
                assert!(ready.drain().is_none());
                drop(binding);
                assert!(drain.peek().is_none());
                assert!(drain.pop().is_none());
            }
            assert!(ready.is_empty());

            let mut fired = Vec::new();
            scope.run_turn(|_context, turn| {
                assert_eq!(turn.ready_round(|value| fired.push(value)), 1);
            });
            assert_eq!(fired.len(), 1);
            assert!(expected.matches(fired[0]));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn bounded_snapshot_preserves_pending_generation() {
    for (backlog, pending) in [(1, false), (2, true)] {
        let queued = schedule::LIMIT + backlog;
        let count = queued + usize::from(pending);
        let config = (Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE))
        .with_ready(count)
        .expect("ready capacity");
        let mut reports = Vec::new();

        Runtime { config }.with_scope(
            |scope| {
                scope.with_driver(|context, mut control| {
                    let dref = context.driver_ref();
                    let targets: Vec<_> = (0..count)
                        .map(|slot| {
                            dref.targets::<KeyTag<ROUTE>>()
                                .bind(SlotIndex::from(slot as u16), Epoch::INITIAL)
                                .dispatch()
                        })
                        .collect();
                    let slots = (targets
                        .iter()
                        .copied()
                        .map(|target| dref.ready().make_slot(target))
                        .collect::<Result<Vec<_>, _>>()
                        .map(Vec::into_boxed_slice))
                    .expect("ready slots");
                    for slot in &slots[..queued] {
                        slot.activate();
                    }

                    {
                        let turn = control.begin();
                        let mut next = 0;
                        let drained = turn.ready_round(|token| {
                            assert!(targets[next].matches(token));
                            next += 1;
                            if pending {
                                slots[queued].activate();
                            }
                        });
                        assert_eq!(drained, schedule::LIMIT);
                        assert_eq!(next, schedule::LIMIT);
                        assert!(turn.is_exhausted());
                        assert!(dref.ready().has_ready());
                    }
                    let turn = control.begin();
                    let mut next = schedule::LIMIT;
                    let drained = turn.ready_round(|token| {
                        assert!(targets[next].matches(token));
                        next += 1;
                    });
                    assert_eq!(drained, backlog);
                    assert_eq!(next, queued);
                    if pending {
                        assert_eq!(
                            turn.ready_round(|token| {
                                assert!(targets[queued].matches(token));
                            }),
                            1
                        );
                    }
                    assert!(!dref.ready().has_ready());
                });
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    }
}
