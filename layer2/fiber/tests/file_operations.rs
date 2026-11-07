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
    manifold::{
        app::Host,
        file::{appender, read},
    },
    runtime::executor::session,
};
use dope_fiber::{abi::Task, file::Operation};
use dope_test::{fibers, harness::scenario};
use o3::mem::Lease;

use common::{
    file::{Capture, Observed, Profile, buffer, fixture, open},
    recover,
    runtime::{self, Setup},
};

#[test]
fn opens_reads_and_returns_the_original_charged_owner() {
    let dir = fixture("fiber_file_read");
    let directory = file::Dir::open(dir.path()).expect("directory owner");
    let request = open(&directory, "data");
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
                let done = session.with_quiesce(
                    &mut finish,
                    Host::new(reader),
                    scenario::TIMEOUT,
                    |mut app| {
                        let opened = fibers::TEST
                            .try_run(
                                &mut app,
                                Operation::new(&port, read::Request::Open(request)),
                            )
                            .expect("open fiber")
                            .ok()
                            .expect("open admission");
                        let read::Completion::Open { request, result } = opened else {
                            panic!("open completion")
                        };
                        drop(request);
                        let file = result.expect("open succeeds");
                        let descriptor = file.as_fd().as_raw_fd();
                        let completed = fibers::TEST
                            .try_run(
                                &mut app,
                                Operation::new(
                                    &port,
                                    read::Request::Metadata(read::Metadata::new(file)),
                                ),
                            )
                            .expect("metadata fiber")
                            .ok()
                            .expect("metadata admission");
                        let read::Completion::Metadata { file, result } = completed else {
                            panic!("metadata completion")
                        };
                        let (kind, length) = result.expect("metadata succeeds");
                        assert_eq!(file.as_fd().as_raw_fd(), descriptor);
                        assert!(kind.is_regular());
                        assert_eq!(length, file::Len::fixed::<3>());
                        let mut request =
                            read::Read::try_new(file, output, file::Offset::fixed::<0>())
                                .ok()
                                .expect("nonempty output");
                        for (offset, expected) in [
                            (file::Offset::fixed::<2>(), b"c".as_slice()),
                            (file::Offset::fixed::<3>(), b"".as_slice()),
                            (file::Offset::fixed::<0>(), b"abc".as_slice()),
                        ] {
                            let completed = fibers::TEST
                                .try_run(
                                    &mut app,
                                    Operation::new(&port, read::Request::Read(request.at(offset))),
                                )
                                .expect("read fiber")
                                .ok()
                                .expect("read admission");
                            let read::Completion::Read {
                                request: returned,
                                result,
                            } = completed
                            else {
                                panic!("read completion")
                            };
                            assert_eq!(result.map(transfer::Len::into_raw), Ok(expected.len()));
                            assert_eq!(returned.output().as_ref(), expected);
                            request = returned;
                        }

                        let missing = open(&directory, "absent");
                        let completed = fibers::TEST
                            .try_run(
                                &mut app,
                                Operation::new(&port, read::Request::Open(missing)),
                            )
                            .expect("error fiber")
                            .ok()
                            .expect("error admission");
                        let read::Completion::Open { result, .. } = completed else {
                            panic!("open error completion")
                        };
                        assert!(matches!(result, Err(read::Error::System(Errno::NOT_FOUND))));
                        let occupied = port
                            .try_request(read::Request::Open(open(&directory, "data")))
                            .ok()
                            .expect("occupy the only slot");
                        let rejected = port
                            .try_request(read::Request::Read(request))
                            .err()
                            .expect("full read admission");
                        let (read::Request::Read(request), cause) = rejected.into_parts() else {
                            panic!("returned read")
                        };
                        assert_eq!(cause, read::Admission::Full);
                        assert_eq!(request.output().as_ref(), b"abc");
                        drop(occupied);
                        (request, descriptor)
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
                assert_eq!(output.item().as_ref(), b"abc");
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
fn queued_cancellation_and_uncollected_completion_preserve_owners() {
    let dir = fixture("fiber_file_queued");
    let path = dir.path().join("data");
    let layout = read::Layout::<KeyTag<42>>::try_new(NonZero::<u32>::MIN).expect("one request");
    let mut finish = session::Completion::new();
    let mut reports = Vec::new();
    Setup::try_for_plan::<Profile, _>(&layout)
        .expect("file layout")
        .with_session(
            |mut session| {
                let returned = Cell::new(None);
                let port = layout.try_port().expect("request slots");
                let reader = read::Reader::<KeyTag<42>, _>::try_new(&port, &mut session.driver())
                    .expect("reader");
                let budget = session
                    .driver()
                    .storage
                    .budget(Resident::bytes(8))
                    .expect("resident budget");
                let output = Lease::open(Capture(&returned), buffer(&budget));
                let request = read::Read::try_new(
                    fs::File::open(&path).expect("file").into(),
                    output,
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
                let done = session.with_quiesce(
                    &mut finish,
                    Host::new(observed),
                    scenario::TIMEOUT,
                    |mut app| {
                        let ticket = port
                            .try_request(read::Request::Read(request))
                            .ok()
                            .expect("first request");
                        drop(ticket);
                        assert!(returned.take().is_none());
                        fibers::TEST
                            .try_run(&mut app, fibers::Yield::new())
                            .expect("bounded cleanup turn");
                        assert!(
                            !submitted.get(),
                            "cancelled queued read never entered native submission"
                        );
                        drop(returned.take().expect("queued owner returned"));
                        let request = read::Read::try_new(
                            fs::File::open(&path).expect("file").into(),
                            Lease::open(Capture(&returned), buffer(&budget)),
                            file::Offset::fixed::<0>(),
                        )
                        .ok()
                        .expect("nonempty read");
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
                        ticket
                    },
                );
                let Ok(ticket) = done else {
                    panic!("uncollected completion permits normal finish")
                };
                assert!(returned.take().is_none());
                drop(ticket);
                let output = returned.take().expect("completed owner returned");
                assert_eq!(output.as_ref(), b"abc");
                drop(output);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            runtime::check_scope,
        );
    runtime::check_reports(&reports);
}

#[test]
fn dropped_submitted_fiber_retains_output_through_forced_finish() {
    let dir = fixture("fiber_file_finish");
    let path = dir.path().join("data");
    let layout = read::Layout::<KeyTag<43>>::try_new(NonZero::<u32>::MIN).expect("one request");
    let mut finish = session::Completion::new();
    let mut reports = Vec::new();
    Setup::try_for_plan::<Profile, _>(&layout)
        .expect("file layout")
        .with_session(
            |mut session| {
                let returned = Cell::new(None);
                let port = layout.try_port().expect("request slots");
                let reader = read::Reader::<KeyTag<43>, _>::try_new(&port, &mut session.driver())
                    .expect("reader");
                let budget = session
                    .driver()
                    .storage
                    .budget(Resident::bytes(8))
                    .expect("resident budget");
                let output = Lease::open(Capture(&returned), buffer(&budget));
                let request = read::Read::try_new(
                    fs::File::open(&path).expect("file").into(),
                    output,
                    file::Offset::fixed::<0>(),
                )
                .ok()
                .expect("nonempty read");
                let submitted = Cell::new(false);
                let observed = Observed {
                    reader,
                    submitted: &submitted,
                    on_completion: None,
                };
                let done = session.with_quiesce(
                    &mut finish,
                    Host::new(observed),
                    Duration::ZERO,
                    |mut app| {
                        let mut operation =
                            Task::new(Operation::new(&port, read::Request::Read(request)));
                        assert!(
                            fibers::TEST
                                .try_run(&mut app, fibers::TEST.poll_once(Pin::new(&mut operation)))
                                .expect("first poll")
                                .is_none()
                        );
                        assert!(submitted.get(), "the original native read owns the buffer");
                        drop(operation);
                        assert!(returned.take().is_none());
                    },
                );
                assert!(
                    done.is_err(),
                    "zero quiesce window exercises forced native finish"
                );
                drop(returned.take().expect("quiescent owner returned"));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            runtime::check_scope,
        );
    assert!(
        reports
            .iter()
            .all(|event| matches!(event, dope::core::io::Reclaimed::Read { .. })),
        "forced cleanup reports only the original read: {reports:?}"
    );
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
                        for request in [
                            read::Request::Open(open(&directory, "data")),
                            read::Request::Prepare(appender::Prepare::new(
                                open(&directory, "data").into_path(),
                                Some(b'c'),
                            )),
                        ] {
                            submitted.set(false);
                            cancelled.set(false);
                            ticket.set(Some(
                                port.try_request(request).ok().expect("open admission"),
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
                        }

                        let request = open(&directory, "data");
                        let completion = fibers::TEST
                            .try_run(
                                &mut app,
                                Operation::new(&port, read::Request::Open(request)),
                            )
                            .expect("reused open slot")
                            .ok()
                            .expect("reused admission");
                        assert!(matches!(
                            completion,
                            read::Completion::Open { result: Ok(_), .. }
                        ));
                    },
                );
                assert!(done.is_ok(), "late open cancellation drains");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            runtime::check_scope,
        );
    runtime::check_reports(&reports);
}
