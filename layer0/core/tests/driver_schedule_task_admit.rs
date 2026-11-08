use dope_core::driver::{life::recovery, schedule::ready::task::topology};

use crate::common::{
    driver::{
        Scoped as _,
        support::{Configure as _, Targets as _},
    },
    wake::Ready as _,
};

pub mod common;

use std::num;

use dope_core::driver::{schedule::ready::task, settings};

use crate::common::{
    driver::task::Index,
    rt::{Runtime, finish, recover},
};

#[test]
fn drop_releases() {
    let config = (Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE))
    .with_ready(2)
    .expect("ready capacity");
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            let dref = scope.context().driver_ref();
            let slot = (dref.ready().make_slot(dref.target())).expect("root ready slot");
            let root = task::Parent::from(slot.target());

            {
                let admit = (root.try_admit()).expect("task slot");
                assert!(matches!(
                    root.try_admit(),
                    Err(task::Error::Capacity {
                        requested: num::NonZero::<usize>::MIN,
                        available: 0
                    })
                ));
                drop(admit);
            }
            assert!(root.try_admit().is_ok(), "released task slot");
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
fn domain_atomic() {
    let config = (Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE))
    .with_ready(65_536 + 3)
    .expect("ready capacity");
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            let dref = scope.context().driver_ref();
            let slot = (dref.ready().make_slot(dref.target())).expect("root ready slot");
            let root = task::Parent::from(slot.target());

            let _reserved = task::pool::Pool::<(), 65_536>::try_new(root).expect("reserved prefix");
            assert!(matches!(
                task::Domain::<(), 3>::try_new(root),
                Err(task::Error::Capacity {
                    requested,
                    available: 2,
                }) if requested == num::NonZero::<usize>::MIN.saturating_add(2)
            ));

            {
                let domain = (task::Domain::<(), 2>::try_new(root))
                    .expect("atomic failure preserved capacity");
                assert!(matches!(
                    root.try_admit(),
                    Err(task::Error::Capacity {
                        requested: num::NonZero::<usize>::MIN,
                        available: 0
                    })
                ));
                drop(domain);
            }

            let first = (root.try_admit()).expect("first returned slot");
            let second = (root.try_admit()).expect("second returned slot");
            assert!(matches!(
                root.try_admit(),
                Err(task::Error::Capacity {
                    requested: num::NonZero::<usize>::MIN,
                    available: 0
                })
            ));
            drop((first, second));

            let pool = task::pool::Pool::<(), 2>::try_new(root)
                .expect("returned chain can be reserved again");
            let root = pool.try_root(root).expect("same live root");
            for _ in 0..2 {
                let first = pool.try_admit(root).expect("chain head");
                let last = pool.try_admit(root).expect("chain tail");
                assert!(matches!(
                    pool.try_admit(root),
                    Err(task::Error::Capacity { available: 0, .. })
                ));
                drop((first, last));
            }
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
fn domain_reclaims() {
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
                let mut domain = (task::Domain::<(), 1>::try_new(root)).expect("one task credit");
                let node = Box::pin(task::raw::Node::new());
                let ready = Box::pin(topology::Slot::new());
                assert!(domain.try_admit().is_ok(), "first domain admission");
                let admit = (domain.try_admit()).expect("reused domain admission");
                // SAFETY: the admitted node and ready set remain pinned through reclaim.
                let binding = unsafe {
                    task::raw::Binding::bind_leased(
                        node.as_ref(),
                        admit,
                        ready
                            .try_attach(root, Index(0))
                            .expect("task topology fixture"),
                    )
                };
                let wake = binding.waker();
                wake.wake();
                scope.run_turn(|_context, turn| {
                    assert_eq!(
                        turn.ready_round(|target| assert!(dref.target().matches(target))),
                        1
                    );
                });
                assert_eq!(ready.pop(), Some(Index(0)));
                // SAFETY: this node was admitted by `domain` immediately above.
                assert!((unsafe { binding.reclaim(&mut domain) }) == (task::Reclaim::Reusable));

                assert!(matches!(
                    root.try_admit(),
                    Err(task::Error::Capacity {
                        requested: num::NonZero::<usize>::MIN,
                        available: 0
                    })
                ));

                // SAFETY: the vacant node and ready set remain pinned through this binding.
                let binding = unsafe {
                    task::raw::Binding::bind_leased(
                        node.as_ref(),
                        domain.try_admit().expect("reclaimed task credit"),
                        ready.try_attach(root, Index(0)).expect("retained topology"),
                    )
                };
                binding.waker().wake();
                drop(binding);
                drop(node);
                assert_eq!(ready.pop(), None);
                assert!(root.try_admit().is_ok(), "node returned its credit");
            }
            assert!(root.try_admit().is_ok(), "domain returned its credit");
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
fn domains_interleave() {
    let config = (Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE))
    .with_ready(4)
    .expect("ready capacity");
    let mut reports = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            let dref = scope.context().driver_ref();
            let slot = (dref.ready().make_slot(dref.target())).expect("root ready slot");
            let root = task::Parent::from(slot.target());
            let first = (task::Domain::<(), 1>::try_new(root)).expect("first domain");
            let second = (task::Domain::<(), 1>::try_new(root)).expect("second domain");
            {
                let global = (root.try_admit()).expect("one global credit remains");
                assert!(matches!(
                    root.try_admit(),
                    Err(task::Error::Capacity {
                        requested: num::NonZero::<usize>::MIN,
                        available: 0
                    })
                ));
                drop(global);
            }
            {
                let restored = (root.try_admit())
                    .expect("released global admission restores only its own credit");
                assert!(matches!(
                    root.try_admit(),
                    Err(task::Error::Capacity {
                        requested: num::NonZero::<usize>::MIN,
                        available: 0
                    })
                ));
                drop(restored);
            }
            drop(first);
            assert!(root.try_admit().is_ok(), "first domain returned one credit");
            drop(second);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
