pub mod common;
use std::{cell, io, iter, mem};

use dope_core::platform::affinity::Cpu;
use dope_runtime::process;
use dope_test::{alloc::Tracker, harness::scenario::process::Test};
use o3::alloc;
use process::failure::{config, create};

use crate::common::process::{Cpus as _, Input, Resources};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
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

#[test]
fn rejects_duplicate_cpus() {
    let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPUs");
    let result = process::Layout::pinned([cpu, cpu]);
    assert!(matches!(
        result,
        Err(create::Error::Config(config::Error::DuplicateCpu(actual))) if actual == cpu
    ));
}

#[test]
fn accepts_unbounded_hint() {
    let [first, second] = <[Cpu; 2]>::try_current().expect("required available CPUs");
    let mut cpus = [first, second].into_iter();
    let cpus = iter::from_fn(move || cpus.next());
    assert!(process::Layout::pinned(cpus).is_ok());
}

#[test]
fn rejects_huge_reservation() {
    let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPUs");
    let result = process::Layout::pinned(iter::repeat(cpu));
    assert!(matches!(
        result,
        Err(create::Error::Allocation(alloc::Error::Overflow))
    ));
}

#[test]
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

fn try_limit() -> io::Result<(libc::rlim_t, libc::rlim_t)> {
    let mut limit = mem::MaybeUninit::uninit();
    // SAFETY: the output points to storage for one complete resource limit.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful query initialized both limit fields.
    let limit = unsafe { limit.assume_init() };
    Ok((limit.rlim_cur, limit.rlim_max))
}

#[test]
fn preserves_nofile_limit() {
    let output = Test {
        name: "preserves_nofile_limit",
    }
    .try_run(|| {
        let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPUs");
        let before = try_limit().expect("query open-file limit");
        assert!(
            process::Layout::pinned([cpu])
                .and_then(|layout| layout.try_assign([()]))
                .is_ok()
        );
        assert_eq!(try_limit().expect("query open-file limit"), before);
    })
    .expect("isolated nofile process");
    if let Some(output) = output {
        assert!(
            output.status.success(),
            "isolated nofile proof failed: {output:?}"
        );
    }
}

#[test]
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

#[test]
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
