pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{cell::Cell, env, fs, io, pin, time::Duration};

use dope_core::{
    driver::{life::recovery, route::KeyTag},
    io::fs::{open, path::CStrExt as _},
};
use dope_manifold::{
    app::{Host, creation},
    file::appender,
    timing,
};
use dope_runtime::executor::session::Completion;
use dope_test::{
    alloc::Tracker,
    fibers::{TEST, Wait},
    harness::{
        scenario::process::{self, Limit, Resource as _, Test},
        support::file,
    },
};
use o3::alloc;

use crate::common::{
    manifold::appender::{FileSize, Native, Observed, Request},
    recover,
    scenario::Runtime,
};

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
    Runtime::try_for_plan::<timing::Throughput, _>(&layout)
        .expect("appender layout")
        .with_scope(
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
                recover,
            ),
        );
    assert_eq!(fs::read(torn).expect("unchanged rejected file"), b"{");
}

#[test]
fn drains_reused_blocks_after_truncation() {
    let base = env::temp_dir();
    let mut finish = Completion::new();
    let dir = file::Directory::try_with(&base, "appender_blocks", |result| {
        result.expect("remove appender fixture")
    })
    .expect("temporary path");
    let path = dir.path().join("journal");
    fs::write(&path, b"keep\nrotated\n").expect("existing complete records");
    let pipepath = dir.path().join("fifo");
    let pipename = Box::try_copy(pipepath.as_path()).expect("fifo path");
    // SAFETY: pipename retains this terminated fixture path through creation.
    let created = unsafe { libc::mkfifo(pipename.as_ptr(), 0o600) };
    assert_eq!(created, 0, "create fifo: {}", io::Error::last_os_error());
    let mut storage = appender::Storage::<2, 64>::try_new().expect("append buffers");
    let mut events = Vec::new();
    let layout = (
        appender::Layout::<KeyTag<36>>::default(),
        appender::Layout::<KeyTag<37>>::default(),
    );

    Runtime::try_for_plan::<timing::Throughput, _>(&layout)
        .expect("appender layout")
        .with_scope(|mut session| {
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
                appender::Appender::<KeyTag<37>, 2, 64>::open(&path, Some(b'\n'), &mut storage, &mut session.driver());
            let mut writer = writer.expect("append file");
            fs::OpenOptions::new().write(true).open(&path)
                .expect("external truncation descriptor")
                .set_len(5).expect("truncate after opening");
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
                let observed = Observed { writer, checked: &checked, native: None };
                let result = session.with_quiesce(&mut finish, Host::new(observed), Duration::from_secs(2), |_| ());
                assert!(result.is_ok(), "shutdown drains both owned blocks");
                assert_eq!(checked.get(), Some(true), "submitted write waits before completion");
            }
            assert!(storage.failure().is_none(), "successful drain replaces cancellation");
        }, recovery::Policy::new(|event| events.push(event), recover));
    assert!(
        events.is_empty(),
        "unexpected reclaimed operations: {events:?}"
    );

    let bytes = fs::read(&path).expect("written file");
    let mut expected = b"keep\n".to_vec();
    expected.extend_from_slice(&[b'a'; 64]);
    expected.extend_from_slice(&[b'b'; 64]);
    assert_eq!(bytes, expected);
}

#[test]
fn retains_native_failure_and_rotates() {
    let child = Test { name: "retains_native_failure_and_rotates" }.run(
        &env::current_exe().expect("test executable"),
        env::var_os(process::CHILD).as_deref(),
        || {
            let dir = file::Directory::try_with(&env::temp_dir(), "appender_failure", |result| {
                result.expect("remove appender fixture")
            }).expect("temporary directory");
            for (name, retry) in [("dropped", false), ("retried", true)] {
                let path = dir.path().join(name);
                let old = dir.path().join(format!("{name}.old"));
                let candidate = dir.path().join(format!("{name}.candidate"));
                let torn = dir.path().join(format!("{name}.torn"));
                fs::write(&torn, b"{").expect("invalid candidate tail");
                let mut storage = appender::Storage::<3, 8>::try_new().expect("three blocks");
                let limit = Limit::<FileSize, _>::try_zero(|result| result.expect("restore limit"))
                    .expect("isolated file-size limit");
                FileSize::soft(&FileSize::limits().expect("current limit"), 3)
                    .expect("three-byte prefix");
                let mut reports = Vec::new();
                Runtime::try_for_plan::<timing::Throughput, _>(
                    &appender::Layout::<KeyTag<37>>::default(),
                ).expect("appender layout").with_scope(|mut session| {
                    let mut writer = appender::Appender::<KeyTag<37>, 3, 8>::open(
                        &path, None, &mut storage, &mut session.driver(),
                    ).expect("initial generation");
                    let mut record = pin::Pin::new(&mut writer).record(8).expect("first block");
                    record.try_extend(b"abcdefgh").expect("first bytes");
                    record.commit();
                    let checked = Cell::new(None);
                    let native = Native::default();
                    let done = native.done.handle();
                    native.request.set(Some(Request::Fill(&candidate)));
                    let mut finish = Completion::new();
                    session.with_quiesce(
                        &mut finish,
                        Host::new(Observed { writer, checked: &checked, native: Some(&native) }),
                        Duration::from_secs(2),
                        |mut app| {
                            assert_eq!(TEST.try_wait(&mut app, &done, 1).expect("native failure"), Wait::Reached);
                            assert_eq!(fs::read(&path).expect("partial generation"), b"abc");
                            let contender = dope_core::io::fs::File::from(
                                fs::OpenOptions::new().write(true).open(&path).expect("contending descriptor"),
                            );
                            assert!(contender.try_lock().is_err(), "failed writer retains its lock");
                            drop(contender);

                            native.request.set(Some(Request::Rotate(&candidate, None)));
                            assert_eq!(TEST.try_wait(&mut app, &done, 2).expect("failed rotation"), Wait::Reached);
                            assert!(matches!(native.rotated.take(), Some(Err(appender::RotateError::Failed(appender::Failure::System(errno)))) if errno.code().get() == libc::EFBIG));
                            assert!(!candidate.exists());
                            limit.try_restore().expect("restore before another native turn");
                            native.request.set(Some(Request::Paused));
                            assert_eq!(TEST.try_wait(&mut app, &done, 3).expect("paused native turn"), Wait::Reached);
                            assert_eq!(fs::read(&path).expect("paused generation"), b"abc");
                            if !retry {
                                return;
                            }

                            fs::rename(&path, &old).expect("rename retained inode");
                            fs::write(&path, []).expect("replacement pathname");
                            native.request.set(Some(Request::Retry));
                            assert_eq!(TEST.try_wait(&mut app, &done, 4).expect("explicit retry drain"), Wait::Reached);
                            assert_eq!(fs::read(&old).expect("retained inode"), b"abcdefghIJKLMNOPqrst");
                            assert!(fs::read(&path).expect("unselected inode").is_empty());

                            native.request.set(Some(Request::Rotate(&torn, Some(b'\n'))));
                            assert_eq!(TEST.try_wait(&mut app, &done, 5).expect("candidate tail rejection"), Wait::Reached);
                            assert!(matches!(native.rotated.take(), Some(Err(appender::RotateError::Open(appender::OpenError::Tail { expected: b'\n', observed: Some(b'{') })))));
                            let candidate_owner = dope_core::io::fs::File::from(
                                fs::OpenOptions::new().write(true).open(&torn).expect("rejected candidate"),
                            );
                            candidate_owner.try_lock().expect("candidate lock released");
                            drop(candidate_owner);
                            native.request.set(Some(Request::Append(b"uv")));
                            assert_eq!(TEST.try_wait(&mut app, &done, 6).expect("old generation remains writable"), Wait::Reached);
                            assert_eq!(fs::read(&old).expect("unchanged generation"), b"abcdefghIJKLMNOPqrstuv");

                            native.request.set(Some(Request::Rotate(&path, None)));
                            assert_eq!(TEST.try_wait(&mut app, &done, 7).expect("healthy rotation"), Wait::Reached);
                            native.rotated.take().expect("rotation result").expect("new locked generation");
                            let previous = dope_core::io::fs::File::from(
                                fs::OpenOptions::new().write(true).open(&old).expect("previous generation"),
                            );
                            previous.try_lock().expect("old descriptor closed");
                            drop(previous);
                            let contender = dope_core::io::fs::File::from(
                                fs::OpenOptions::new().write(true).open(&path).expect("current generation"),
                            );
                            assert!(contender.try_lock().is_err(), "new generation stays locked");
                            drop(contender);
                            native.request.set(Some(Request::Append(b"new\n")));
                            assert_eq!(TEST.try_wait(&mut app, &done, 8).expect("new generation drain"), Wait::Reached);
                        },
                    ).expect("native appender retirement");
                    if retry {
                        assert!(storage.failure().is_none());
                    } else {
                        assert!(matches!(storage.failure(), Some(appender::Failure::System(errno)) if errno.code().get() == libc::EFBIG));
                    }
                }, recovery::Policy::new(|event| reports.push(event), recover));
                assert!(reports.is_empty(), "unexpected native reclamation: {reports:?}");
                if retry {
                    assert_eq!(fs::read(&old).expect("old bytes after retirement"), b"abcdefghIJKLMNOPqrstuv");
                    assert_eq!(fs::read(&path).expect("new bytes after retirement"), b"new\n");
                } else {
                    assert_eq!(fs::read(&path).expect("failed bytes after retirement"), b"abc");
                }
                let released = dope_core::io::fs::File::from(
                    fs::OpenOptions::new().write(true).open(&path).expect("retired generation"),
                );
                released.try_lock().expect("retirement closes the owner");
            }
        },
    );
    if let Some(mut child) = child {
        let output = child.output().expect("isolated appender child");
        assert!(output.status.success(), "{output:?}");
    }
}
