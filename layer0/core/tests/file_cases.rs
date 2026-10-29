use std::convert::Infallible;

use dope_core::driver::{
    life::{creation, recovery},
    route::KeyTag,
};
use dope_test::scope::Error;

use crate::common::file::{Retain as _, Submit as _};

pub mod common;

use std::{
    env,
    fs::{File, OpenOptions, read_dir, write},
    ops,
    os::fd::AsFd as _,
    time::Duration,
};

use dope_core::{
    Errno, driver,
    driver::{
        Context, life,
        life::domain,
        result::{
            poll,
            poll::{Drain, fault},
        },
        route,
        route::{Epoch, Operation, Tag, Target, indices::SlotIndex, kind, kind::Class},
        schedule::state::scope,
        settings::QueueLayout,
    },
    io::{
        Event, Reclaimed,
        event::{Kind, file::open},
        fs,
        fs::{
            Submission,
            open::{Access, Creation, Options},
            path,
        },
    },
};
use dope_test::harness::support::file;

use crate::common::{
    file::{Identity, Inspect as _},
    rt,
};

type Route = KeyTag<7>;

fn target<'d>(driver: driver::Reference<'d>, n: u16) -> Target<'d, Route> {
    driver
        .targets::<Route>()
        .bind(SlotIndex::from(n), Epoch::INITIAL)
}

/// # Safety
/// The caller retains request through this exact scope's final quiescence.
unsafe fn submit_unobserved_open<'a, 'd>(
    scope: &mut life::Scope<'a, 'd>,
    request: &fs::OpenRequest,
) {
    scope.with_driver(|context, mut ctl| {
        let mut turn = ctl.begin();
        let mut context = context.retained();
        let owner = target(context.driver_ref(), 0);
        // SAFETY: the backing owner outlives the runtime scope and its final
        // quiescence, including assertion failure and early-return paths.
        unsafe { context.submit(request.submission(owner)) }.expect("retained fixture");
        assert!(
            driver::Context::wait(
                context.driver(),
                turn.reactor(),
                Some(Duration::from_secs(1)),
            )
            .is_ok()
        );
    });
}

fn drive_until<'d, R: Tag, C: Class>(
    driver: &mut Context<'_, 'd>,
    turn: &mut scope::Guard<'_, 'd>,
    want: Operation<'d, R, C>,
    mut target: impl FnMut(&Kind<'d>) -> Option<route::Erased<'d, C>>,
) -> Option<Kind<'d>> {
    let mut drain = Drain::Drained;
    for _ in 0..500 {
        if drain == Drain::Drained {
            assert!(
                driver::Context::wait(driver, turn.reactor(), Some(Duration::from_millis(20)))
                    .is_ok()
            );
        }
        let mut matched = None;
        drain = dispatch_all(driver, turn.reactor(), |event| {
            let event = event.into_kind();
            if target(&event).is_some_and(|target| want.matches(target)) {
                assert!(matched.replace(event).is_none());
            }
        })
        .expect("file dispatch");
        if let Some(event) = matched {
            return Some(event);
        }
    }
    None
}

#[test]
fn sync_completes() {
    use dope_core::io::fs::Sync;

    let base = env::temp_dir();
    let tmp = file::Directory::try_with(&base, "data_sync", cleanup).expect("temporary directory");
    let path = tmp.path().join("data");
    write(&path, b"durable").expect("fixture data");
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open writable file");
    let mut reports = Vec::new();

    rt::Runtime::quic::<2, 8>().with_turn(
        |turn, mut driver| {
            let target = target(driver.driver_ref(), 2);
            let done = target.operation::<kind::Sync>();
            // SAFETY: the backing owner outlives the runtime scope and its final
            // quiescence, including assertion failure and early-return paths.
            unsafe {
                driver.submit(Submission::<Route, kind::Sync>::sync(
                    file.as_fd(),
                    Sync::Data,
                    target,
                ))
            }
            .expect("retained fixture");

            let event = drive_until(driver.driver(), turn, done, |event| match event {
                Kind::Sync(done) => Some(done.token()),
                _ => None,
            });
            assert!(event.is_some(), "completion for sync operation");
            let Some(event) = event else {
                return;
            };
            assert!(matches!(
                event,
                Kind::Sync(done) if matches!(done.outcome(), Ok(()))
            ));
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
fn creates_component() {
    let base = env::temp_dir();
    let root =
        file::Directory::try_with(&base, "component_create", cleanup).expect("temporary path");
    let dir = (fs::Dir::open(root.path())).expect("directory capability");
    let name = "x".repeat(255);
    let part = (path::Component::parse_os(name.as_ref())).expect("bounded component");
    let request = fs::OpenRequest::new(
        dir.confine_name(part).expect("component allocation"),
        Options::new(Access::ReadWrite).creation(Creation::Exclusive),
    );
    let existing = fs::OpenRequest::new(
        dir.confine_name(path::Component::parse_os(name.as_ref()).expect("component"))
            .expect("confined path"),
        Options::new(Access::Read),
    );
    let mut reports = Vec::new();

    rt::Runtime::quic::<2, 8>().with_turn(
        |turn, mut driver| {
            for (attempt, request) in [&request, &request, &existing].into_iter().enumerate() {
                let target = target(driver.driver_ref(), attempt as u16 + 1);
                let done = target.operation::<kind::Open>();
                // SAFETY: the backing owner outlives the runtime scope and its final
                // quiescence, including assertion failure and early-return paths.
                unsafe { driver.submit(request.submission(target)) }.expect("retained fixture");
                let event = drive_until(driver.driver(), turn, done, |event| match event {
                    Kind::Open(done) => Some(done.token()),
                    _ => None,
                });
                assert!(event.is_some(), "completion for component creation");
                let Some(event) = event else { return };
                let done = match event {
                    Kind::Open(done) => Some(done),
                    _ => None,
                }
                .expect("creation must return its open completion");
                match done.into_parts().1 {
                    open::Outcome::Opened(file) => {
                        assert_ne!(attempt, 1);
                        let file = File::from(file);
                        let metadata = file.metadata().expect("created file metadata");
                        assert!(metadata.is_file());
                        assert_eq!(metadata.len(), 0);
                    }
                    open::Outcome::Failed(errno) => {
                        assert_eq!(attempt, 1);
                        assert_eq!(errno, Errno::ALREADY_EXISTS);
                    }
                }
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
fn finish_drains_opens() {
    const OPEN_COUNT: usize = 192;

    let base = env::temp_dir();
    let root = file::Directory::try_with(&base, "quiescence_cq_overflow", cleanup)
        .expect("temporary path");
    let dir = (fs::Dir::open(root.path())).expect("directory capability");
    let mut requests = Vec::with_capacity(OPEN_COUNT);
    let mut reports = Vec::new();

    for index in 0..OPEN_COUNT {
        let name = format!("opened-{index}");
        let name = (path::Component::parse_os(name.as_ref())).expect("component");
        let path = (dir.confine_name(name)).expect("confined path allocation");
        requests.push(fs::OpenRequest::new(
            path,
            Options::new(Access::ReadWrite).creation(Creation::Exclusive),
        ));
    }

    rt::Runtime::quic::<2, 8>()
        .queue_layout(QueueLayout::fixed::<256, 256>())
        .with_scope(
            |scope| {
                scope.with_driver(|context, mut ctl| {
                    let mut turn = ctl.begin();
                    let mut driver = context.retained();
                    for (index, request) in requests.iter().enumerate() {
                        let owner = target(driver.driver_ref(), index as u16);
                        // SAFETY: the backing owner outlives the runtime scope and its final
                        // quiescence, including assertion failure and early-return paths.
                        unsafe { driver.submit(request.submission(owner)) }
                            .expect("retained fixture");
                    }
                    assert!(
                        driver::Context::wait(
                            driver.driver(),
                            turn.reactor(),
                            Some(Duration::from_secs(1)),
                        )
                        .is_ok()
                    );
                });
                scope.quiesce();
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert_eq!(reports.len(), OPEN_COUNT);
    let mut echoes = Vec::with_capacity(OPEN_COUNT);
    let mut opened = 0;
    for event in reports {
        let (echo, result, close) = match event {
            Reclaimed::Open {
                echo,
                result,
                close,
            } => Some((echo, result, close)),
            _ => None,
        }
        .expect("every reclaimed operation is a submitted open");
        assert!(!echoes.contains(&echo), "each open retires exactly once");
        echoes.push(echo);
        match result {
            Ok(()) => {
                assert_eq!(close, Some(Ok(())), "close each opened descriptor");
                opened += 1;
            }
            Err(error) => {
                assert_eq!(error, Errno::CANCELED);
                assert_eq!(close, None, "a canceled open created no descriptor");
            }
        }
    }

    let entries = read_dir(root.path());
    let entries = (entries).expect("required fixture outcome");
    let mut created = 0;
    for entry in entries {
        let entry = (entry).expect("required fixture outcome");
        assert!(
            Identity::try_new(&entry.path())
                .expect("descriptor fixture")
                .collect(0..4096)
                .is_empty()
        );
        created += 1;
    }
    assert!(created > 0);
    assert_eq!(created, opened);
}

#[test]
fn reaps_unobserved_open() {
    let base = env::temp_dir();
    let root = file::Directory::try_with(&base, "unobserved_open_quiescence", cleanup)
        .expect("temporary path");
    let opened = root.path().join("opened");
    let dir = (fs::Dir::open(root.path())).expect("directory capability");
    let name = (path::Component::parse_os("opened".as_ref())).expect("component");
    let path = (dir.confine_name(name)).expect("confined path allocation");
    let request = fs::OpenRequest::new(
        path,
        Options::new(Access::ReadWrite).creation(Creation::Exclusive),
    );
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(rt::Runtime::quic::<2, 8>().config());
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    // SAFETY: request remains owned outside this domain until final quiescence.
                    unsafe { submit_unobserved_open(&mut scope, &request) };
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        matches!(
            reports.as_slice(),
            [Reclaimed::Open {
                result: Ok(()),
                close: Some(Ok(())),
                ..
            }]
        ),
        "the completed open must report successful descriptor cleanup: {reports:?}"
    );

    assert!(
        Identity::try_new(&opened)
            .expect("descriptor fixture")
            .collect(0..4096)
            .is_empty()
    );
}

#[test]
fn rejects_long_component() {
    let part = "x".repeat(256);
    assert!(matches!(
        path::Component::parse_os(part.as_ref()),
        Err(path::Error::Component {
            offset: 0,
            length,
            limit: 255,
        }) if length == part.len()
    ));
}

fn dispatch_all<'d>(
    driver: &mut driver::Context<'_, 'd>,
    work: scope::Reactor<'_, 'd>,
    mut dispatch: impl FnMut(Event<'d>),
) -> Result<poll::Drain, fault::Dispatch> {
    let out = driver::Context::dispatch(driver, work, |event, _source| {
        let event = event;
        dispatch(event);
        ops::ControlFlow::Continue(())
    });
    match out {
        poll::Dispatch::Ready { drain, retained } => {
            assert!(retained.is_none());
            Ok(drain)
        }
        poll::Dispatch::Fault(error) => Err(error),
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}

fn cleanup(result: Result<(), file::Remove>) {
    result.expect("fixture directory cleanup");
}
