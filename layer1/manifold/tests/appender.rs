pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{cell::Cell, convert::Infallible, env, fs, io, pin, time::Duration};

use dope_core::{
    driver::{
        life::{creation::Attempt, domain, recovery},
        route::KeyTag,
    },
    io::fs::{open, path::CStrExt as _},
};
use dope_manifold::{
    app::{Host, creation},
    file::appender,
    timing,
};
use dope_runtime::executor::{create, session::Completion};
use dope_test::{alloc::Tracker, harness::support::file};
use o3::alloc;

use crate::common::{manifold::appender::Observed, scenario::Runtime};

#[test]
fn storage_releases_prefix() {
    for fail in 0..3 {
        let (result, activity) =
            Tracker::measure(|| Tracker::fail_after(fail, appender::Storage::<3, 64>::try_new));
        assert!(matches!(
            result,
            Err(creation::Error::Allocation(alloc::Error::Exhausted))
        ));
        assert_eq!(activity, [fail, fail * 64, fail, fail * 64]);
    }
}

#[test]
fn tail_precedes_route_admission() {
    let base = env::temp_dir();
    let dir = file::Directory::try_with(&base, "appender_tail", |result| {
        result.expect("remove appender fixture")
    })
    .expect("temporary path");
    let empty = dir.path().join("empty");
    let torn = dir.path().join("torn");
    let complete = dir.path().join("complete");
    fs::write(&torn, b"{").expect("torn record");
    fs::write(&complete, b"{}\n").expect("complete record");
    let mut storage = appender::Storage::<1, 64>::try_new().expect("append buffer");
    let mut other = appender::Storage::<1, 64>::try_new().expect("other append buffer");
    let layout = (
        appender::Layout::<KeyTag<38>>::default(),
        appender::Layout::<KeyTag<39>>::default(),
    );
    let mut create = Attempt::new();
    let executor = Runtime::try_for_plan::<timing::Throughput, _>(&layout)
        .expect("appender layout")
        .try_executor(&mut create);
    executor
        .as_ref()
        .map_err(create::Error::kind)
        .expect("native creation");
    let executor = executor.ok().expect("native creation");
    executor.with_storage(layout).run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let held = appender::Appender::<KeyTag<38>, 1, 64>::open(
                    &empty,
                    Some(b'\n'),
                    &mut storage,
                    &mut session.driver(),
                )
                .expect("empty file passes");
                {
                    let rejected = appender::Appender::<KeyTag<38>, 1, 64>::open(
                        &torn,
                        Some(b'\n'),
                        &mut other,
                        &mut session.driver(),
                    );
                    assert!(matches!(
                        rejected,
                        Err(creation::Error::Appender(appender::OpenError::Tail {
                            expected: b'\n',
                            observed: Some(b'{'),
                        }))
                    ));
                }
                let file = dope_core::io::fs::File::from(
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&torn)
                        .expect("reopen rejected file"),
                );
                file.try_lock().expect("tail rejection released ownership");
                drop(file);
                drop(held);
                drop(
                    appender::Appender::<KeyTag<39>, 1, 64>::open(
                        &complete,
                        Some(b'\n'),
                        &mut other,
                        &mut session.driver(),
                    )
                    .expect("complete tail passes"),
                );
            },
            recovery::Policy::new(
                |event| panic!("unexpected reclaimed operation: {event:?}"),
                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("native recovery: {:?}", failure.cause())
                },
            ),
        );
        result
            .as_ref()
            .map_err(domain::Error::cause)
            .expect("native retirement");
        result.ok().expect("native retirement")
    });
    assert_eq!(fs::read(torn).expect("unchanged rejected file"), b"{");
}

#[test]
fn drains_reused_blocks() {
    let base = env::temp_dir();
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let dir = file::Directory::try_with(&base, "appender_blocks", |result| {
        result.expect("remove appender fixture")
    })
    .expect("temporary path");
    let path = dir.path().join("journal");
    let pipepath = dir.path().join("fifo");
    let pipename = Box::try_copy(pipepath.as_path()).expect("fifo path");
    // SAFETY: pipename retains this terminated fixture path through creation.
    let created = unsafe { libc::mkfifo(pipename.as_ptr(), 0o600) };
    assert_eq!(created, 0, "create fifo: {}", io::Error::last_os_error());
    let storage = appender::Storage::<2, 64>::try_new();
    let mut storage = storage.expect("append buffers");
    let mut events = Vec::new();
    let layout = (
        appender::Layout::<KeyTag<36>>::default(),
        appender::Layout::<KeyTag<37>>::default(),
    );

    let executor = Runtime::try_for_plan::<timing::Throughput, _>(&layout)
        .expect("appender layout")
        .try_executor(&mut create);
    executor
        .as_ref()
        .map_err(create::Error::kind)
        .expect("native creation");
    let executor = executor.ok().expect("native creation");
    executor.with_storage(layout).run(|executor| {
        let result = executor.enter_with(|mut session| {
            {
                let rejected = appender::Appender::<KeyTag<37>, 2, 64>::open(
                    &pipepath, None, &mut storage, &mut session.driver(),
                );
                assert!(matches!(
                    rejected,
                    Err(creation::Error::Appender(appender::OpenError::Open(
                        open::Error::System(errno)
                    ))) if errno.code().get() == libc::ENXIO
                ));
            }
            {
                let rejected = Tracker::fail_after(0, || {
                    appender::Appender::<KeyTag<37>, 2, 64>::open(&path, None, &mut storage, &mut session.driver())
                });
                assert!(matches!(
                    rejected,
                    Err(creation::Error::Appender(appender::OpenError::Open(
                        open::Error::Allocation(alloc::Error::Exhausted)
                    )))
                ));
            }
            {
                let mut dropped = appender::Appender::<KeyTag<36>, 2, 64>::open(
                    dir.path().join("abandoned"), None, &mut storage, &mut session.driver(),
                ).expect("uninstalled appender");
                for _ in 0..2 {
                    let mut record = pin::Pin::new(&mut dropped).record(64).expect("abandoned block");
                    record.try_extend(&[b'x'; 64]).expect("abandoned bytes");
                    record.commit();
                }
            }
            assert!(matches!(storage.failure(), Some(appender::Failure::Cancelled)));
            let writer =
                appender::Appender::<KeyTag<37>, 2, 64>::open(&path, None, &mut storage, &mut session.driver());
            let mut writer = writer.expect("append file");
            let ((), activity) = Tracker::measure(|| {
                Tracker::fail_after(0, || {
                    let mut empty = pin::Pin::new(&mut writer).record(0).expect("empty record");
                    empty.try_extend(b"").expect("empty fragment");
                    let error = empty.try_extend(b"x").expect_err("zero limit");
                    assert_eq!((error.current, error.added, error.maximum), (0, 1, 0));
                    empty.commit();
                    {
                        let mut record = pin::Pin::new(&mut writer).record(4).expect("reserved record");
                        record.try_extend(b"lost").expect("discarded record bytes");
                    }
                })
            });
            assert_eq!(activity, [0; 4]);
            for byte in *b"ab" {
                let ((), activity) = Tracker::measure(|| Tracker::fail_after(0, || {
                    let mut record = pin::Pin::new(&mut writer).record(64).expect("available block");
                    record.try_extend(&[byte; 32]).expect("first fragment");
                    let error = record.try_extend(&[byte; 33]).expect_err("complete fragment rejected");
                    assert_eq!((error.current, error.added, error.maximum), (32, 33, 64));
                    record.commit();
                    let mut record = pin::Pin::new(&mut writer).record(32).expect("remaining block suffix");
                    record.try_extend(&[byte; 32]).expect("remaining fragment");
                    record.commit();
                }));
                assert_eq!(activity, [0; 4], "record borrows reserved storage");
            }
            {
                let (rejected, allocs) = Tracker::usage(|| pin::Pin::new(&mut writer).record(1));
                assert_eq!(allocs, (0, 0));
                assert!(
                    matches!(rejected, Err(appender::WriteError::Full(full)) if full.capacity() == 2)
                );
            }
            {
                let checked = Cell::new(None);
                let observed = Observed { writer, checked: &checked };
                let result = session.with_quiesce(&mut finish, Host::new(observed), Duration::from_secs(2), |_| ());
                assert!(result.is_ok(), "shutdown drains both owned blocks");
                assert_eq!(checked.get(), Some(true), "submitted write waits before completion");
            }
            assert!(storage.failure().is_none(), "successful drain replaces cancellation");
        }, recovery::Policy::new(|event| events.push(event), |failure: recovery::Recovery<'_, '_>| -> Infallible {
            panic!("native recovery: {:?}", failure.cause())
        }));
        result.as_ref().map_err(domain::Error::cause).expect("native retirement");
        result.ok().expect("native retirement")
    });
    assert!(
        events.is_empty(),
        "unexpected reclaimed operations: {events:?}"
    );

    let bytes = fs::read(&path);
    let bytes = (bytes).expect("written file");
    let mut expected = [b'a'; 64].to_vec();
    expected.extend_from_slice(&[b'b'; 64]);
    assert_eq!(bytes, expected);
}
