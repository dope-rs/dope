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

use common::{
    file::{Observed, Profile, buffer, fixture, open},
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
                let output = buffer(&budget, b'!');
                let address = output.as_ref().as_ptr();
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
                        let read::Completion::Open { result, .. } = opened else {
                            panic!("open completion")
                        };
                        let file = result.expect("open succeeds");
                        let descriptor = file.as_fd().as_raw_fd();
                        let request = read::Read::try_new(file, output, file::Offset::fixed::<0>())
                            .ok()
                            .expect("nonempty output");
                        let completed = fibers::TEST
                            .try_run(
                                &mut app,
                                Operation::new(&port, read::Request::Read(request)),
                            )
                            .expect("read fiber")
                            .ok()
                            .expect("read admission");
                        let read::Completion::Read { request, result } = completed else {
                            panic!("read completion")
                        };
                        assert_eq!(result, Ok(transfer::Len::fixed::<3>()));
                        let completed = fibers::TEST
                            .try_run(
                                &mut app,
                                Operation::new(
                                    &port,
                                    read::Request::Read(
                                        request.at(file::Offset::from(transfer::Len::fixed::<3>())),
                                    ),
                                ),
                            )
                            .expect("EOF fiber")
                            .ok()
                            .expect("EOF admission");
                        let read::Completion::Read { request, result } = completed else {
                            panic!("EOF completion")
                        };
                        assert_eq!(result, Ok(transfer::Len::ZERO));
                        let (file, output, _) = request.into_parts();
                        assert_eq!(file.as_fd().as_raw_fd(), descriptor);
                        assert_eq!(output.as_ref().as_ptr(), address);
                        assert_eq!(output.as_ref(), b"abc!!!!!");
                        assert_eq!(budget.resident(), 8);
                        drop((file, output));
                        assert_eq!(budget.resident(), 0);

                        let request = open(&directory, "absent");
                        let completed = fibers::TEST
                            .try_run(
                                &mut app,
                                Operation::new(&port, read::Request::Open(request)),
                            )
                            .expect("error fiber")
                            .ok()
                            .expect("error admission");
                        let read::Completion::Open { result, .. } = completed else {
                            panic!("open error completion")
                        };
                        assert!(matches!(result, Err(read::Error::System(Errno::NOT_FOUND))));
                    },
                );
                assert!(done.is_ok(), "application finalizes");
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
                let port = layout.try_port().expect("request slots");
                let reader = read::Reader::<KeyTag<42>>::try_new(&port, &mut session.driver())
                    .expect("reader");
                let budget = session
                    .driver()
                    .storage
                    .budget(Resident::bytes(8))
                    .expect("resident budget");
                let output = buffer(&budget, 0);
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
                        let directory = file::Dir::open(dir.path()).expect("directory");
                        let next = open(&directory, "data");
                        let denied = port
                            .try_request(read::Request::Open(next))
                            .err()
                            .expect("one occupied request slot");
                        assert_eq!(*denied.error(), read::Admission::Full);
                        let (next, _) = denied.into_parts();
                        drop(ticket);
                        assert_eq!(
                            budget.resident(),
                            8,
                            "ticket cancellation leaves cleanup ownership in the installed pool"
                        );
                        fibers::TEST
                            .try_run(&mut app, fibers::Yield::new())
                            .expect("bounded cleanup turn");
                        assert!(
                            !submitted.get(),
                            "cancelled queued read never entered native submission"
                        );
                        assert_eq!(budget.resident(), 0);
                        let completion = fibers::TEST
                            .try_run(&mut app, Operation::new(&port, next))
                            .expect("reused slot fiber")
                            .ok()
                            .expect("reused slot admission");
                        let read::Completion::Open { result, .. } = completion else {
                            panic!("open completion")
                        };
                        let request = read::Read::try_new(
                            result.expect("reused slot opens"),
                            buffer(&budget, 0),
                            file::Offset::fixed::<0>(),
                        )
                        .ok()
                        .expect("nonempty read");
                        completed.set(false);
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
                assert_eq!(budget.resident(), 8, "the reply still owns its output");
                drop(ticket);
                assert_eq!(budget.resident(), 0, "reply Drop releases its output");
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
                let port = layout.try_port().expect("request slots");
                let reader = read::Reader::<KeyTag<43>>::try_new(&port, &mut session.driver())
                    .expect("reader");
                let budget = session
                    .driver()
                    .storage
                    .budget(Resident::bytes(8))
                    .expect("resident budget");
                let output = buffer(&budget, 0);
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
                        assert_eq!(
                            budget.resident(),
                            8,
                            "future Drop cannot reclaim accepted native output"
                        );
                    },
                );
                assert!(
                    done.is_err(),
                    "zero quiesce window exercises forced native finish"
                );
                assert_eq!(
                    budget.resident(),
                    0,
                    "completed native quiescence releases the cancelled owner"
                );
                let directory = file::Dir::open(dir.path()).expect("directory");
                let request = open(&directory, "data");
                let denied = port
                    .try_request(read::Request::Open(request))
                    .err()
                    .expect("closed after finish");
                assert_eq!(*denied.error(), read::Admission::Closed);
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
                        let request = open(&directory, "data");
                        ticket.set(Some(
                            port.try_request(read::Request::Open(request))
                                .ok()
                                .expect("open admission"),
                        ));
                        // The fixture drops this exact ticket only when the original
                        // successful Open CQ owns its new FD, immediately before Reader.
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
