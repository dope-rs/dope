pub mod common;

use std::{cell::Cell, mem};

use common::{
    fork::{Owner, Rejected},
    process::Cpus as _,
};
use dope_core::platform::{affinity, affinity::Cpu};
use dope_runtime::process::{self, failure::input};
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn moves_one_worker() {
    let cpus = <[Cpu; 1]>::try_current().expect("required available CPUs");
    let layout = (process::Layout::pinned(cpus)).expect("one CPU");
    let drops = Cell::new(Some(0));
    let copies = Cell::new(0);
    let owner = Owner {
        drops: &drops,
        value: 41,
    };
    {
        let (runtime, usage) = Tracker::usage(|| {
            layout.try_fork(
                owner,
                [1],
                |owner, value| {
                    copies.set(copies.get() + 1);
                    Ok::<_, Rejected>(Owner {
                        drops: owner.drops,
                        value: owner.value + value,
                    })
                },
                |mut owner, value| {
                    owner.value += value;
                    assert_eq!(owner.value, 42);
                    owner
                },
            )
        });
        assert!(runtime.is_ok(), "single worker input");
        assert_eq!(copies.get(), 0);
        assert_eq!(usage, (0, 0));
        assert_eq!(drops.get(), Some(0));
    }
    assert_eq!(drops.get(), Some(1));
}

#[test]
fn orders_replicas() {
    let cpus = <[Cpu; 3]>::try_current().expect("required available CPUs");
    let layout = (process::Layout::pinned(cpus)).expect("three CPUs");
    let drops = Cell::new(Some(0));
    let copies = Cell::new(0);
    let owner = Owner {
        drops: &drops,
        value: 41,
    };
    {
        let (runtime, usage) = Tracker::usage(|| {
            layout.try_fork(
                owner,
                [1, 2, 3],
                |owner, value| {
                    copies.set(copies.get() + 1);
                    assert_eq!(value, copies.get());
                    Ok::<_, Rejected>(Owner {
                        drops: owner.drops,
                        value: owner.value + value,
                    })
                },
                |mut owner, value| {
                    assert_eq!(copies.get(), 2);
                    assert_eq!(value, 3);
                    owner.value += value;
                    owner
                },
            )
        });
        assert!(runtime.is_ok(), "three worker inputs");
        assert_eq!(usage, (1, 2 * mem::size_of::<(affinity::Cpu, Owner<'_>)>()));
        assert_eq!(drops.get(), Some(0));
    }
    assert_eq!(drops.get(), Some(3));
}

#[test]
fn retains_replica_failure() {
    let cpus = <[Cpu; 3]>::try_current().expect("required available CPUs");
    let layout = (process::Layout::pinned(cpus)).expect("three CPUs");
    let drops = Cell::new(Some(0));
    let finished = Cell::new(0);
    let owner = Owner {
        drops: &drops,
        value: 41,
    };
    let error = layout.try_fork(
        owner,
        [1, 2, 3],
        |owner, value| {
            if value == 2 {
                return Err(Rejected(owner.value + value));
            }
            Ok(Owner {
                drops: owner.drops,
                value: owner.value + value,
            })
        },
        |owner, _| {
            finished.set(finished.get() + 1);
            owner
        },
    );
    assert!(
        matches!(error, Err(input::Error::Replica { cpu, cause: Rejected(43) }) if cpu == cpus[1])
    );
    assert_eq!(finished.get(), 0);
    assert_eq!(drops.get(), Some(2));
}
