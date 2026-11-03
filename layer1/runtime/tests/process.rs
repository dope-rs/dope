pub mod common;
use std::{cell, env, iter, mem, process::Command};

use dope_core::platform::affinity::Cpu;
use dope_runtime::process;
use dope_test::{
    alloc::Tracker,
    checks::{Error, Suite},
    harness::{scenario::process::Test, support::file::Directory},
};
use o3::alloc;
use process::failure::{config, create};

use crate::common::process::{Cpus as _, Input, Resources, try_limit};

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

#[test]
fn preserves_nofile_limit() {
    let output = Test {
        name: "preserves_nofile_limit",
    }
    .run(
        &std::env::current_exe().expect("current test executable"),
        std::env::var_os(dope_test::harness::scenario::process::CHILD).as_deref(),
        || {
            let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPU");
            let before = try_limit().expect("query open-file limit");
            assert!(
                process::Layout::pinned([cpu])
                    .and_then(|layout| layout.try_assign([()]))
                    .is_ok()
            );
            assert_eq!(try_limit().expect("query open-file limit"), before);
        },
    )
    .map(|mut child| child.output().expect("isolated nofile process"));
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

#[test]
fn cancels_gated_startup() -> Result<(), Error> {
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &[
            "dope-core",
            "dope-gen",
            "dope-manifold",
            "dope-net",
            "dope-runtime",
            "dope-test",
            "libc",
            "o3",
            "pin-project",
        ],
    );
    let exe = env::current_exe().expect("current test executable");
    let output = suite
        .try_build(&exe, env!("CARGO_PKG_NAME"))?
        .output()
        .expect("run process");
    let artifacts = suite.try_collect(&exe, output)?;
    let directory = Directory::try_with(
        exe.parent().expect("test executable directory"),
        "startup-cancel",
        |result| result.expect("remove startup cancellation executable"),
    )
    .expect("startup cancellation executable directory");
    let binary = directory.path().join("startup_cancel");
    let mut compiler = artifacts.compiler();
    compiler.arg(format!("--emit=link={}", binary.display()));
    let compiled = compile(
        &mut compiler,
        r#"pub mod common;
#[global_allocator]
static ALLOCATOR: dope_test::alloc::Tracker = dope_test::alloc::Tracker::new();
fn main() { common::process::cancel_gated_startup(); }
"#,
    );
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let output = Command::new(&binary).output().expect("run process");
    assert!(output.status.success(), "startup cancellation: {output:?}");
    directory.try_close().expect("remove compiler output");
    Ok(())
}

fn compile(command: &mut std::process::Command, source: &str) -> std::process::Output {
    let mut child = command.spawn().expect("start compiler");
    let written = child
        .stdin
        .take()
        .map(|mut input| std::io::Write::write_all(&mut input, source.as_bytes()));
    let output = child.wait_with_output().expect("reap compiler");
    written
        .expect("compiler stdin")
        .expect("write compiler source");
    output
}
