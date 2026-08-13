use std::{ffi, os::fd::AsFd, path::Path, time::Duration};

use dope_core::{
    driver::{
        self, Context, ops,
        route::{self, Epoch, KeyTag, Operation, SlotIndex, Tag, Target, Token},
    },
    io::{
        Event, ReadEvent, Sync,
        event::{Kind, open},
        fs::{self, Native, OpenPath, Submission},
    },
};
use dope_test::{
    file,
    scenario::rt::{self, RetainedTurn},
};

use crate::{dispatch_all, open_fds_for, submit_retained};

const ROUTE: u8 = 7;

fn target<'d>(driver: driver::Reference<'d>, n: u16) -> Target<'d, KeyTag<ROUTE>> {
    driver
        .targets::<KeyTag<ROUTE>>()
        .bind(SlotIndex::from(n), Epoch::INITIAL)
}

fn confined(path: &Path) -> OpenPath {
    let parent = path.parent().expect("file parent");
    let name = path
        .file_name()
        .and_then(ffi::OsStr::to_str)
        .expect("utf-8 file name");
    fs::Directory::open(parent)
        .expect("directory capability")
        .relative(name)
        .expect("relative file path")
}

fn event_token(ev: &Event) -> Token {
    match ev.kind() {
        Kind::Accept(t, ..)
        | Kind::Read(t, ..)
        | Kind::Write(t, ..)
        | Kind::Stat(t, ..)
        | Kind::Sync(t, ..) => *t,
        Kind::Recv(completion) => completion.token(),
        Kind::Send(completion) => completion.token(),
        Kind::Socket(completion) => completion.token(),
        Kind::Tuning(completion) => completion.token(),
        Kind::Connect(completion) => completion.token(),
        Kind::Open(completion) => completion.token(),
        Kind::Shutdown => panic!("unexpected shutdown completion"),
    }
}

fn drive_until<'d, R: Tag>(
    driver: &mut Context<'_, 'd>,
    turn: &mut RetainedTurn<'_, '_, 'd>,
    want: Operation<'d, R>,
) -> Event<'d> {
    for _ in 0..500 {
        let _ = ops::poll::Poll::wait(driver, turn.reactor(), Some(Duration::from_millis(20)));
        let mut matched = None;
        let _ = dispatch_all(driver, turn.reactor(), |event| {
            if want.matches(event_token(&event)) {
                assert!(matched.replace(event).is_none());
            }
        });
        if let Some(event) = matched {
            return event;
        }
    }
    panic!("no completion for token");
}

#[test]
fn async_read_returns_file_bytes() {
    let payload = b"the quick brown fox jumps over the lazy dog";
    let tmp = file::File::with("read", payload);

    rt::Runtime::quic(2, 8).with_retained_turn(|mut turn, mut driver| {
        let f = std::fs::File::open(tmp.path()).expect("open");
        assert_eq!(f.metadata().expect("metadata").len(), payload.len() as u64);

        let cases = [
            (0u64, payload.len(), Some(payload.len())),
            (payload.len() as u64, 16, None),
            (0, 4096, Some(payload.len())),
        ];
        for (i, (offset, buf_len, want)) in cases.into_iter().enumerate() {
            let mut dst = vec![0_u8; buf_len];
            let target = target(driver.driver_ref(), i as u16 + 1);
            let completion = target.operation(route::kind::READ);
            submit_retained(
                &mut driver,
                Submission::<Native, KeyTag<ROUTE>>::read(f.as_fd(), &mut dst, offset, target)
                    .expect("representable read length"),
            )
            .expect("push read");

            let event = drive_until(driver.driver(), &mut turn, completion);
            match (event.kind(), want) {
                (Kind::Read(_, ReadEvent::Read(n)), Some(len)) => {
                    assert_eq!(*n as usize, len);
                    assert_eq!(&dst[..*n as usize], payload);
                }
                (Kind::Read(_, ReadEvent::Eof), None) => {}
                _ => panic!("case {i}: unexpected event: {}", variant(&event)),
            }
        }
    });
}

#[test]
fn async_data_sync_reaches_a_durability_completion() {
    let tmp = file::File::with("data_sync", b"durable");

    rt::Runtime::quic(2, 8).with_retained_turn(|mut turn, mut driver| {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path())
            .expect("open writable file");
        let target = target(driver.driver_ref(), 2);
        let completion = target.operation(route::kind::SYNC);
        submit_retained(
            &mut driver,
            Submission::<Native, KeyTag<ROUTE>>::sync(file.as_fd(), fs::Sync::Data, target),
        )
        .expect("push sync");

        let event = drive_until(driver.driver(), &mut turn, completion);
        assert!(matches!(event.kind(), Kind::Sync(_, Sync::Done)));
    });
}

#[test]
fn async_open_missing_path_reports_enoent() {
    rt::Runtime::quic(2, 8).with_retained_turn(|mut turn, mut driver| {
        let root = file::Directory::with("missing_open_root");
        let path = fs::Directory::open(root.path())
            .expect("directory capability")
            .relative("definitely-missing")
            .expect("relative path");
        let request = path.regular_request();
        let target = target(driver.driver_ref(), 3);
        let completion = target.operation(route::kind::OPEN);
        submit_retained(&mut driver, request.submission(target)).expect("push open");

        let event = drive_until(driver.driver(), &mut turn, completion);
        match event.kind() {
            Kind::Open(completion) => {
                let open::Outcome::Failed(errno) = completion.outcome() else {
                    panic!("expected open failure, got {}", variant(&event));
                };
                assert_eq!(*errno, libc::ENOENT);
            }
            _ => panic!("expected open failure, got {}", variant(&event)),
        }
    });
}

#[test]
fn final_quiescence_drains_unobserved_opens_without_leaking_files() {
    const OPEN_COUNT: usize = 192;

    let root = file::Directory::with("quiescence_cq_overflow");
    let directory = fs::Directory::open(root.path()).expect("directory capability");
    let requests = (0..OPEN_COUNT)
        .map(|_| {
            directory
                .relative("opened")
                .expect("relative path")
                .regular_request()
        })
        .collect::<Vec<_>>();
    let opened = root.path().join("opened");
    std::fs::write(&opened, b"cq overflow").expect("test file");
    let before = open_fds_for(&opened);

    rt::Runtime::quic(2, 8)
        .queue_layout(driver::settings::QueueLayout::fixed::<256, 256>())
        .with_driver_scope(|scope| {
            let retained_owner = std::pin::pin!(());
            scope.with_turn(|_, context, mut controller| {
                let turn = controller.begin(driver::schedule::MAX_TURN_WORK_BUDGET);
                let mut driver = crate::retained_context(context, retained_owner.as_ref());
                for (index, request) in requests.iter().enumerate() {
                    let owner = target(driver.driver_ref(), index as u16);
                    submit_retained(&mut driver, request.submission(owner)).expect("submit open");
                }
                drop(turn);
            });
            let finalization = scope.final_quiescence().expect("final quiescence");
            drop(finalization);
        });

    assert_eq!(open_fds_for(&opened), before);
}

#[test]
fn confined_path_rejects_oversized_component() {
    let root = file::Directory::with("oversized_component_root");
    let directory = fs::Directory::open(root.path()).expect("directory capability");
    let component = "x".repeat(256);
    let error = match directory.relative(&component) {
        Ok(_) => panic!("oversized component was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "dope: confined path exceeds the platform limit"
    );
}

#[test]
fn confined_path_rejects_oversized_path() {
    let root = file::Directory::with("oversized_path_root");
    let directory = fs::Directory::open(root.path()).expect("directory capability");
    let path = "x/".repeat(libc::PATH_MAX as usize / 2 + 1);
    let error = match directory.relative(&path) {
        Ok(_) => panic!("oversized path was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "dope: confined path exceeds the platform limit"
    );
}

#[test]
fn confined_path_semantics_precede_platform_limits() {
    let root = file::Directory::with("invalid_oversized_path_root");
    let directory = fs::Directory::open(root.path()).expect("directory capability");
    let path = format!("{}/..", "x".repeat(256));
    let error = match directory.relative(&path) {
        Ok(_) => panic!("parent traversal was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "dope: path is not a confined relative path"
    );
}

#[test]
fn async_open_then_read_via_returned_fd() {
    let payload = b"opened-by-io-uring";
    let tmp = file::File::with("open_read", payload);

    rt::Runtime::quic(2, 8).with_retained_turn(|mut turn, mut driver| {
        let cpath = confined(tmp.path());
        let request = cpath.regular_request();
        let open_target = target(driver.driver_ref(), 10);
        let open_completion = open_target.operation(route::kind::OPEN);
        submit_retained(&mut driver, request.submission(open_target)).expect("push open");

        let event = drive_until(driver.driver(), &mut turn, open_completion);
        let fd = match event.into_kind() {
            Kind::Open(completion) => match completion.into_parts().1 {
                open::Outcome::Opened(file) => file.into_owned(),
                open::Outcome::Failed(_) => panic!("expected open success"),
            },
            _ => panic!("expected open success"),
        };

        let mut dst = vec![0_u8; payload.len()];
        let read_target = target(driver.driver_ref(), 11);
        let read_completion = read_target.operation(route::kind::READ);
        submit_retained(
            &mut driver,
            Submission::<Native, KeyTag<ROUTE>>::read(fd.as_fd(), &mut dst, 0, read_target)
                .expect("representable read length"),
        )
        .expect("push read");

        let event = drive_until(driver.driver(), &mut turn, read_completion);
        let n = match event.kind() {
            Kind::Read(_, ReadEvent::Read(n)) => *n as usize,
            _ => panic!("expected read, got {}", variant(&event)),
        };
        assert_eq!(&dst[..n], payload);
    });
}

fn variant(ev: &Event) -> &'static str {
    match ev.kind() {
        Kind::Open(..) => "Open",
        Kind::Read(..) => "Read",
        Kind::Write(..) => "Write",
        Kind::Stat(..) => "Stat",
        Kind::Sync(..) => "Sync",
        Kind::Accept(..) => "Accept",
        Kind::Recv(..) => "Recv",
        Kind::Send(..) => "Send",
        Kind::Socket(..) => "Socket",
        Kind::Tuning(..) => "Tuning",
        Kind::Connect(..) => "Connect",
        Kind::Shutdown => "Shutdown",
    }
}
