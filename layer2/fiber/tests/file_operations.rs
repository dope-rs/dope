pub mod common;

use std::{
    cell::Cell,
    fs,
    num::NonZero,
    os::fd::{AsFd as _, AsRawFd as _},
    pin::Pin,
    time::Duration,
};

use dope::{
    core::{
        Errno,
        driver::{life::recovery, route::KeyTag, storage::Resident},
        io::{
            fs::{self as file},
            transfer,
        },
    },
    manifold::{app::Host, file::read},
    runtime::executor::session,
};
use dope_fiber::{abi::Task, file::Operation};
use dope_test::{fibers, harness::scenario};
use o3::mem::{Lease, Rejected};

use common::{
    file::{Capture, Coordinate, Observed, Profile, buffer, fixture, open},
    recover,
    runtime::{self, Setup},
};

#[test]
fn opens_reads_and_returns_the_original_charged_owner() {
    let dir = fixture("fiber_file_read");
    let bundle = dir.path().join("bundle");
    let retired = dir.path().join("retired");
    fs::create_dir(&bundle).expect("bundle directory");
    fs::rename(dir.path().join("data"), bundle.join("data")).expect("bundle contents");
    let directory = file::Dir::open(dir.path()).expect("directory owner");
    let name = file::path::Component::parse_os("bundle".as_ref()).expect("bundle name");
    let request = file::OpenRequest::directory(directory.confine_name(name).expect("bundle path"));
    let layout =
        read::Layout::<KeyTag<41>>::try_new(NonZero::<u32>::MIN).expect("request capacity");
    let mut finish = session::Completion::new();
    let mut reports = Vec::new();
    Setup::try_for_plan::<Profile, _>(&layout)
        .expect("file layout")
        .with_session(
            |mut session| {
                let port = layout.try_port().expect("request slots");
                let reader = read::Reader::<KeyTag<41>>::try_new(&port, &mut session.driver())
                    .expect("installed reader");
                let budget = session
                    .driver()
                    .storage
                    .budget(Resident::bytes(8))
                    .expect("resident budget");
                let output = Lease::open(read::Release::default(), buffer(&budget));
                let address = output.item().as_ref().as_ptr();
                let input = Cell::new(None);
                let completed = Cell::new(None);
                let waker = Cell::new(None);
                let done = session.with_quiesce(
                    &mut finish,
                    Coordinate {
                        reader,
                        ticket: None,
                        input: &input,
                        output: &completed,
                        waker: &waker,
                    },
                    scenario::TIMEOUT,
                    |mut app| {
                        let (port, input, completed, waker) =
                            (&port, &input, &completed, &waker);
                        fibers::TEST.try_run(&mut app, dope_fiber::fiber!('_ => async move {
                            let read::Completion::Directory { path, result } =
                                Operation::new(&port, read::Request::Directory(request))
                                    .await.map_err(Rejected::discard).expect("directory admission")
                            else {
                                panic!("directory completion")
                            };
                            drop(path);
                            let directory = result.expect("opened directory");
                            fs::rename(&bundle, &retired).expect("retire directory name");
                            fs::create_dir(&bundle).expect("replacement directory");
                            fs::write(bundle.join("data"), b"wrong").expect("replacement contents");
                            let name = file::path::Component::parse_os("data".as_ref())
                                .expect("file name");
                            let request = file::OpenRequest::directory(
                                directory.confine_name(name).expect("confined file"),
                            );
                            let read::Completion::Directory { result, .. } =
                                Operation::new(&port, read::Request::Directory(request))
                                    .await.map_err(Rejected::discard).expect("directory error admission")
                            else {
                                panic!("directory error completion")
                            };
                            assert!(matches!(result, Err(read::Error::System(Errno::NOT_DIRECTORY))));
                            let read::Completion::Open { request, result } =
                                Operation::new(&port, read::Request::Open(open(&directory, "data")))
                                    .await.map_err(Rejected::discard).expect("open admission")
                            else {
                                panic!("open completion")
                            };
                            drop(request);
                            let file = result.expect("open succeeds");
                            let descriptor = file.as_fd().as_raw_fd();
                            let read::Completion::Metadata { file, result } = Operation::new(
                                &port, read::Request::Metadata(read::Metadata::new(file)),
                            ).await.ok().expect("metadata admission") else {
                                panic!("metadata completion")
                            };
                            let (kind, length, identity) = result.expect("metadata succeeds");
                            assert_eq!(file.as_fd().as_raw_fd(), descriptor);
                            assert!(kind.is_regular());
                            assert_eq!(length, file::Len::fixed::<3>());
                            assert!(identity.is_some());
                            let mut request =
                                read::Read::try_new(file, output, file::Offset::fixed::<0>())
                                    .ok().expect("nonempty output");
                            for (offset, append, count, expected) in [
                                (file::Offset::fixed::<2>(), false, 1, b"c".as_slice()),
                                (file::Offset::fixed::<3>(), false, 0, b"".as_slice()),
                                (file::Offset::fixed::<0>(), false, 3, b"abc".as_slice()),
                                (file::Offset::fixed::<3>(), true, 0, b"abc".as_slice()),
                                (file::Offset::fixed::<0>(), true, 3, b"abcabc".as_slice()),
                                (file::Offset::fixed::<0>(), true, 2, b"abcabcab".as_slice()),
                            ] {
                                request = if append {
                                    request.try_append_at(offset).ok().expect("remaining output")
                                } else {
                                    request.at(offset)
                                };
                                input.set(Some(read::Request::Read(request)));
                                let completion = dope_fiber::abi::poll_fn(|context| {
                                    match completed.take() {
                                        Some(output) => std::task::Poll::Ready(output),
                                        None => {
                                            waker.set(Some(context.as_ref().completion()));
                                            std::task::Poll::Pending
                                        }
                                    }
                                }).await;
                                let read::Completion::Read { request: returned, result } = completion
                                else {
                                    panic!("read completion")
                                };
                                assert_eq!(result.map(transfer::Len::into_raw), Ok(count));
                                assert_eq!(returned.output().as_ref(), expected);
                                request = returned;
                            }
                            let (request, cause) = request.try_append_at(file::Offset::fixed::<0>())
                                .err().expect("full suffix").into_parts();
                            assert_eq!(cause, read::SizeError::Empty);
                            let read::Completion::Open { result, .. } = Operation::new(
                                &port, read::Request::Open(open(&directory, "absent")),
                            ).await.ok().expect("error admission") else {
                                panic!("open error completion")
                            };
                            assert!(matches!(result, Err(read::Error::System(Errno::NOT_FOUND))));
                            let next = open(&directory, "renamed");
                            let mut rename = read::Request::Rename {
                                directory,
                                old: file::path::Component::parse_os("data".as_ref())
                                    .expect("old name").try_to_owned().expect("old name owner"),
                                new: file::path::Component::parse_os("renamed".as_ref())
                                    .expect("new name").try_to_owned().expect("new name owner"),
                            };
                            for expected in [Ok(()), Err(read::Error::System(Errno::NOT_FOUND))] {
                                let read::Completion::Rename { directory, old, new, result } =
                                    Operation::new(&port, rename).await.ok().expect("rename admission")
                                else {
                                    panic!("rename completion")
                                };
                                assert_eq!(result, expected);
                                rename = read::Request::Rename { directory, old, new };
                            }
                            let occupied = port
                                .try_request(read::Request::Open(next))
                                .ok().expect("occupy the only slot");
                            let rejected = port.try_request(read::Request::Read(request))
                                .err().expect("full read admission");
                            let (read::Request::Read(request), cause) = rejected.into_parts() else {
                                panic!("returned read")
                            };
                            assert_eq!(cause, read::Admission::Full);
                            assert_eq!(request.output().as_ref(), b"abcabcab");
                            drop(occupied);
                            (request, descriptor)
                        })).expect("file operations")
                    },
                );
                let Ok((request, descriptor)) = done else {
                    panic!("application finalizes")
                };
                let rejected = port
                    .try_request(read::Request::Read(request))
                    .err()
                    .expect("closed read admission");
                let (read::Request::Read(request), cause) = rejected.into_parts() else {
                    panic!("returned read")
                };
                assert_eq!(cause, read::Admission::Closed);
                let (file, output, _) = request.into_parts();
                assert_eq!(output.item().as_ref(), b"abcabcab");
                assert_eq!(file.as_fd().as_raw_fd(), descriptor);
                assert_eq!(output.item().as_ref().as_ptr(), address);
                assert_eq!(budget.resident(), 8);
                drop((file, output));
                assert_eq!(budget.resident(), 0);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            runtime::check_scope,
        );
    runtime::check_reports(&reports);
}

#[test]
fn cancellation_and_uncollected_completion_preserve_owners() {
    let dir = fixture("fiber_file_cancel");
    let path = dir.path().join("data");
    let layout = read::Layout::<KeyTag<42>>::try_new(NonZero::<u32>::MIN).expect("one request");
    for active in [false, true] {
        let mut finish = session::Completion::new();
        let mut reports = Vec::new();
        Setup::try_for_plan::<Profile, _>(&layout)
            .expect("file layout")
            .with_session(
                |mut session| {
                    let returned = Cell::new(None);
                    let port = layout.try_port().expect("request slots");
                    let reader =
                        read::Reader::<KeyTag<42>, _>::try_new(&port, &mut session.driver())
                            .expect("reader");
                    let budget = session
                        .driver()
                        .storage
                        .budget(Resident::bytes(8))
                        .expect("resident budget");
                    let request = read::Read::try_new(
                        fs::File::open(&path).expect("file").into(),
                        Lease::open(Capture(&returned), buffer(&budget)),
                        file::Offset::fixed::<0>(),
                    )
                    .ok()
                    .expect("nonempty read");
                    let submitted = Cell::new(false);
                    let completed = Cell::new(false);
                    let observe = || {
                        completed.set(true);
                        false
                    };
                    let observed = Observed {
                        reader,
                        submitted: &submitted,
                        on_completion: Some(&observe),
                    };
                    let timeout = if active {
                        Duration::ZERO
                    } else {
                        scenario::TIMEOUT
                    };
                    let done = session.with_quiesce(
                        &mut finish,
                        Host::new(observed),
                        timeout,
                        |mut app| {
                            if active {
                                let mut operation =
                                    Task::new(Operation::new(&port, read::Request::Read(request)));
                                assert!(
                                    fibers::TEST
                                        .try_run(
                                            &mut app,
                                            fibers::TEST.poll_once(Pin::new(&mut operation))
                                        )
                                        .expect("first poll")
                                        .is_none()
                                );
                                assert!(
                                    submitted.get(),
                                    "the original native read owns the buffer"
                                );
                                drop(operation);
                                assert!(returned.take().is_none());
                                return None;
                            }
                            drop(
                                port.try_request(read::Request::Read(request))
                                    .ok()
                                    .expect("first request"),
                            );
                            assert!(returned.take().is_none());
                            fibers::TEST
                                .try_run(&mut app, fibers::Yield::new())
                                .expect("bounded cleanup turn");
                            assert!(
                                !submitted.get(),
                                "queued cancellation never entered native submission"
                            );
                            let output = returned.take().expect("queued owner returned");
                            assert_eq!(output.as_ref(), &[0; 8]);
                            let request = read::Read::try_new(
                                fs::File::open(&path).expect("file").into(),
                                Lease::open(Capture(&returned), output),
                                file::Offset::fixed::<0>(),
                            )
                            .ok()
                            .expect("reused output");
                            let ticket = port
                                .try_request(read::Request::Read(request))
                                .ok()
                                .expect("reused read slot");
                            fibers::TEST
                                .try_run(
                                    &mut app,
                                    dope_fiber::abi::poll_fn(|context| {
                                        if completed.get() {
                                            std::task::Poll::Ready(())
                                        } else {
                                            context.wake();
                                            std::task::Poll::Pending
                                        }
                                    }),
                                )
                                .expect("read completes without polling its ticket");
                            Some(ticket)
                        },
                    );
                    if active {
                        assert!(done.is_err(), "zero quiesce window forces native finish");
                    } else {
                        let Ok(ticket) = done else {
                            panic!("uncollected completion permits normal finish")
                        };
                        assert!(returned.take().is_none());
                        drop(ticket);
                    }
                    let output = returned.take().expect("quiescent owner returned");
                    assert_eq!(output.as_ref(), &[0; 8]);
                    drop(output);
                    assert_eq!(budget.resident(), 0);
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                runtime::check_scope,
            );
        if active {
            assert!(
                reports
                    .iter()
                    .all(|event| matches!(event, dope::core::io::Reclaimed::Read { .. })),
                "forced cleanup reports only the original read: {reports:?}"
            );
        } else {
            runtime::check_reports(&reports);
        }
    }
}

#[test]
fn cancelled_open_reclaims_an_already_opened_descriptor() {
    let dir = fixture("fiber_file_late_open");
    let directory = file::Dir::open(dir.path()).expect("directory owner");
    let layout = read::Layout::<KeyTag<44>>::try_new(NonZero::<u32>::MIN).expect("one request");
    let mut finish = session::Completion::new();
    let mut reports = Vec::new();
    Setup::try_for_plan::<Profile, _>(&layout)
        .expect("file layout")
        .with_session(
            |mut session| {
                let port = layout.try_port().expect("request slots");
                let reader = read::Reader::<KeyTag<44>>::try_new(&port, &mut session.driver())
                    .expect("reader");
                let submitted = Cell::new(false);
                let ticket = Cell::new(None);
                let cancelled = Cell::new(false);
                let cancel = || {
                    if let Some(ticket) = ticket.take() {
                        drop(ticket);
                        cancelled.set(true);
                        true
                    } else {
                        false
                    }
                };
                let observed = Observed {
                    reader,
                    submitted: &submitted,
                    on_completion: Some(&cancel),
                };
                let done = session.with_quiesce(
                    &mut finish,
                    Host::new(observed),
                    scenario::TIMEOUT,
                    |mut app| {
                        ticket.set(Some(
                            port.try_request(read::Request::Open(open(&directory, "data")))
                                .ok()
                                .expect("open admission"),
                        ));
                        // Cancel immediately before Reader consumes the successful Open CQ.
                        fibers::TEST
                            .try_run(
                                &mut app,
                                dope_fiber::abi::poll_fn(|context| {
                                    if cancelled.get() {
                                        std::task::Poll::Ready(())
                                    } else {
                                        context.wake();
                                        std::task::Poll::Pending
                                    }
                                }),
                            )
                            .expect("successful detached Open dispatch");
                        assert!(submitted.get(), "the original Open flight was submitted");
                        assert!(
                            cancelled.get(),
                            "the successful Open cancelled its exact ticket"
                        );

                        drop(
                            port.try_request(read::Request::Open(open(&directory, "data")))
                                .ok()
                                .expect("reused admission"),
                        );
                    },
                );
                assert!(done.is_ok(), "late open cancellation drains");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            runtime::check_scope,
        );
    runtime::check_reports(&reports);
}
