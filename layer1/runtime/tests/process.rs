pub mod common;
use std::{cell, convert::Infallible, iter, mem, rc::Rc, time};

use dope_core::{
    Backend,
    dope_sys::io::fd::Fd as _,
    driver::life::{creation::Attempt, domain, recovery},
    platform::affinity::Cpu,
};
use dope_runtime::{
    executor::{self, Build as _},
    process::{self, lifecycle},
};
use dope_test::alloc::Tracker;
use o3::{alloc, depot::capacity};
use process::failure::{config, create};

use crate::common::process::{Cpus as _, Input, Resources};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

fn main() {
    rejects_partial_workers();
    rejects_duplicate_cpus();
    accepts_unbounded_hint();
    rejects_huge_reservation();
    reports_alloc_failure();
    reports_startup_failure();
    preserves_nofile_limit();
    rejects_invalid_iterator();
    maps_without_growth();
    common::process::cancel_gated_startup();
    common::process::repeated_install();
    common::process::workers_shutdown();
    common::process::terminal_failure();
}

fn rejects_partial_workers() {
    let [first, second] = <[Cpu; 2]>::try_current().expect("required available CPUs");
    let layout = process::Layout::pinned([first, second]).expect("two distinct CPUs");
    let mapped = cell::Cell::new(0);
    let result = layout.try_map([()], |()| {
        mapped.set(mapped.get() + 1);
    });
    assert!(matches!(
        result,
        Err(create::Error::Config(config::Error::ResourceCount {
            actual: 1,
            required: 2
        }))
    ));
    assert_eq!(mapped.get(), 0);
}

fn rejects_duplicate_cpus() {
    let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPUs");
    let result = process::Layout::pinned([cpu, cpu]);
    assert!(matches!(
        result,
        Err(create::Error::Config(config::Error::DuplicateCpu(actual))) if actual == cpu
    ));
}

fn accepts_unbounded_hint() {
    let [first, second] = <[Cpu; 2]>::try_current().expect("required available CPUs");
    let mut cpus = [first, second].into_iter();
    let cpus = iter::from_fn(move || cpus.next());
    assert!(process::Layout::pinned(cpus).is_ok());
}

fn rejects_huge_reservation() {
    let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPUs");
    let result = process::Layout::pinned(iter::repeat(cpu));
    assert!(matches!(
        result,
        Err(create::Error::Allocation(alloc::Error::Overflow))
    ));
}

fn reports_alloc_failure() {
    let cpus = <[Cpu; 2]>::try_current().expect("required available CPUs");
    let result = Tracker::fail_after(0, || process::Layout::pinned(cpus));
    assert!(matches!(
        result,
        Err(create::Error::Allocation(alloc::Error::Exhausted))
    ));
    let layout = process::Layout::pinned(cpus).expect("two CPU topology fits");
    let mapped = cell::Cell::new(0);
    let result = Tracker::fail_after(0, || {
        layout.try_map([1_u64, 2], |input| {
            mapped.set(mapped.get() + 1);
            input
        })
    });
    assert!(matches!(
        result,
        Err(create::Error::Allocation(alloc::Error::Exhausted))
    ));
    assert_eq!(mapped.get(), 0);
}

fn reports_startup_failure() {
    for allocation in [true, false] {
        let (main, worker) = lifecycle::Main::try_pair().expect("lifecycle pair");
        let outcome = worker.run(|source| {
            let mut attempt = Attempt::new();
            if allocation {
                let result = Tracker::fail_after(0, || {
                    source.try_build(&mut attempt, common::runtime::topology::PROFILE_CONFIG)
                });
                assert!(matches!(
                    result,
                    Err(executor::create::Error::Driver(
                        dope_core::driver::result::CreateError::Allocation(alloc::Error::Exhausted)
                    ))
                ));
            } else {
                let executor = source
                    .try_build(&mut attempt, common::runtime::topology::PROFILE_CONFIG)
                    .unwrap_or_else(|error| panic!("executor: {:?}", error.kind()));
                let borrowed = cell::Cell::new(7);
                let plan = common::runtime::executor::storage::Collision {
                    capacity: capacity::Count::new(1),
                    borrowed: &borrowed,
                    local: Rc::new(()),
                };
                executor.with_plan(plan).run(|executor| {
                    let result =
                        executor.try_create(|_| (), recovery::Policy::new(drop, common::recover));
                    assert!(matches!(result, Err(domain::Error::Storage(_))));
                });
            }
            assert!(matches!(
                main.wait_ready(time::Duration::ZERO),
                Err(lifecycle::WaitError::Event(
                    process::failure::bus::Event::Failed
                ))
            ));
            Ok::<_, Infallible>(())
        });
        assert!(matches!(
            outcome,
            lifecycle::Outcome::Failed {
                output: Ok(()),
                report: process::failure::bus::Report::Clean,
            }
        ));
    }
}

fn preserves_nofile_limit() {
    let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPU");
    let before = Backend::try_limit().expect("query open-file limit");
    assert!(
        process::Layout::pinned([cpu])
            .and_then(|layout| layout.try_assign([()]))
            .is_ok()
    );
    assert_eq!(Backend::try_limit().expect("query open-file limit"), before);
}

fn rejects_invalid_iterator() {
    let cpus = <[Cpu; 2]>::try_current().expect("required available CPUs");
    for actual in [0, 1, 3, usize::MAX] {
        let dropped = cell::Cell::new(Some(0));
        let mapped = cell::Cell::new(0);
        let result = process::Layout::pinned(cpus).and_then(|layout| {
            layout.try_map(
                Resources {
                    values: 0..actual,
                    declared: 2,
                },
                |ordinal| {
                    assert_eq!(ordinal, mapped.get());
                    mapped.set(mapped.get() + 1);
                    Input(&dropped)
                },
            )
        });
        assert!(matches!(
            result,
            Err(create::Error::Config(config::Error::ResourceIterator {
                yielded,
                required: 2,
            })) if yielded == actual.min(3)
        ));
        assert_eq!(mapped.get(), actual.min(2));
        assert_eq!(
            dropped.get(),
            Some(mapped.get()),
            "release every mapped owner on failure"
        );
    }
}

fn maps_without_growth() {
    let cpus = <[Cpu; 2]>::try_current().expect("required available CPUs");
    let layout = process::Layout::pinned(cpus).expect("two distinct CPUs");
    let ordinal = cell::Cell::new(0);
    let (runtime, allocs) = Tracker::usage(|| {
        layout.try_map([11_u64, 22], |input| {
            assert_eq!(input, (ordinal.get() + 1) * 11);
            ordinal.set(ordinal.get() + 1);
            input
        })
    });
    assert!(runtime.is_ok());
    assert_eq!(ordinal.get(), 2);
    assert_eq!(allocs, (1, 2 * mem::size_of::<u64>()));
    assert_eq!(
        mem::size_of::<process::Runtime<u64>>(),
        5 * mem::size_of::<usize>()
    );
}
