use std::os::fd::AsRawFd;
use std::time::Duration;

use dope_core::backend::{RawSqe, RetainedSqe, StableSqeSource};
use dope_core::driver::DriverContext;
use dope_core::driver::completion::Completion;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::{Epoch, SlotIndex, Token};
use dope_core::io::file::{self, OpenPath, OsFile};
use dope_core::io::{Event, OpenEvent, ReadEvent, WriteEvent};
use dope_test::{TempFile, with_driver};

struct TestSubmission(RawSqe);

// SAFETY: every test waits for the matching completion before touching or
// dropping the descriptor's backing resources.
unsafe impl StableSqeSource for TestSubmission {
    fn into_raw(self) -> RawSqe {
        self.0
    }
}

fn tok(n: u16) -> Token {
    Token::new(7, SlotIndex::from(n), Epoch::INITIAL)
}

fn event_token(ev: &Event) -> Token {
    match ev {
        Event::Accept(t, ..)
        | Event::Recv(t, ..)
        | Event::Send(t, ..)
        | Event::Timer(t, ..)
        | Event::Socket(t, ..)
        | Event::Connect(t, ..)
        | Event::Write(t, ..)
        | Event::Sync(t, ..)
        | Event::Open(t, ..)
        | Event::Read(t, ..)
        | Event::Stat(t, ..) => *t,
        Event::Shutdown => panic!("unexpected shutdown completion"),
    }
}

fn drive_until<'d>(driver: &mut DriverContext<'_, 'd>, want: Token) -> Event<'d> {
    let mut buf = [const { None }; 32];
    for _ in 0..500 {
        let _ = driver.wait(Some(Duration::from_millis(20)));
        let n = driver.drain(&mut buf);
        for event in &mut buf[..n] {
            let ev = event.take().expect("completion slot");
            let t = event_token(&ev);
            if t.same_target(want) {
                return ev;
            }
        }
    }
    panic!("no completion for token");
}

#[test]
fn async_read_returns_file_bytes() {
    let payload = b"the quick brown fox jumps over the lazy dog";
    let tmp = TempFile::with("read", payload);

    with_driver(|mut driver| {
        let f = OsFile::open(tmp.path_str()).expect("open");
        assert_eq!(f.try_len().expect("len"), payload.len() as u64);

        let cases = [
            (0u64, payload.len(), Some(payload.len())),
            (payload.len() as u64, 16, None),
            (0, 4096, Some(payload.len())),
        ];
        for (i, (offset, buf_len, want)) in cases.into_iter().enumerate() {
            let mut dst = vec![0u8; buf_len];
            let t = tok(i as u16 + 1);
            driver
                .push_retained(RetainedSqe::from_stable(TestSubmission(RawSqe::read(
                    f.fd(),
                    &mut dst,
                    offset,
                    t,
                ))))
                .expect("push read");

            let event = drive_until(&mut driver, t);
            match (&event, want) {
                (Event::Read(_, ReadEvent::Read(n)), Some(len)) => {
                    assert_eq!(*n as usize, len);
                    assert_eq!(&dst[..*n as usize], payload);
                }
                (Event::Read(_, ReadEvent::Eof), None) => {}
                _ => panic!("case {i}: unexpected event: {}", variant(&event)),
            }
        }
    });
}

#[test]
fn async_open_missing_path_reports_enoent() {
    with_driver(|mut driver| {
        let path = OpenPath::new("/nonexistent/dope/definitely/missing/file").expect("path");
        let t = tok(3);
        driver
            .push_retained(RetainedSqe::from_stable(TestSubmission(
                path.open_at(file::O_RDONLY | file::O_CLOEXEC, t),
            )))
            .expect("push open");

        let event = drive_until(&mut driver, t);
        match &event {
            Event::Open(_, OpenEvent::Failed(errno)) => {
                assert_eq!(*errno, libc::ENOENT);
            }
            _ => panic!("expected open failure, got {}", variant(&event)),
        }
    });
}

#[test]
fn async_open_then_read_via_returned_fd() {
    let payload = b"opened-by-io-uring";
    let tmp = TempFile::with("open_read", payload);

    with_driver(|mut driver| {
        let cpath = OpenPath::new(tmp.path_str()).expect("path");
        let to = tok(10);
        driver
            .push_retained(RetainedSqe::from_stable(TestSubmission(
                cpath.open_at(file::O_RDONLY | file::O_CLOEXEC, to),
            )))
            .expect("push open");

        let event = drive_until(&mut driver, to);
        let fd = match event {
            Event::Open(_, OpenEvent::Opened(fd)) => fd,
            _ => panic!("expected open success"),
        };

        let mut dst = vec![0u8; payload.len()];
        let tr = tok(11);
        driver
            .push_retained(RetainedSqe::from_stable(TestSubmission(RawSqe::read(
                fd.as_raw_fd(),
                &mut dst,
                0,
                tr,
            ))))
            .expect("push read");

        let event = drive_until(&mut driver, tr);
        let n = match &event {
            Event::Read(_, ReadEvent::Read(n)) => *n as usize,
            _ => panic!("expected read, got {}", variant(&event)),
        };
        assert_eq!(&dst[..n], payload);
    });
}

#[test]
fn write_path_still_works() {
    let tmp = TempFile::with("write", b"");
    let f = OsFile::create(tmp.path_str()).expect("create");
    let payload = b"write-path-intact";

    with_driver(|mut driver| {
        let t = tok(30);
        driver
            .push_retained(RetainedSqe::from_stable(TestSubmission(RawSqe::write_fd(
                f.fd(),
                payload,
                0,
                t,
            ))))
            .expect("push write");

        let event = drive_until(&mut driver, t);
        match &event {
            Event::Write(_, WriteEvent::Wrote(n)) => assert_eq!(*n as usize, payload.len()),
            _ => panic!("write did not complete: {}", variant(&event)),
        }

        drop(f);
        let back = std::fs::read(tmp.path_str()).expect("read back");
        assert_eq!(&back, payload);
    });
}

fn variant(ev: &Event) -> &'static str {
    match ev {
        Event::Open(..) => "Open",
        Event::Read(..) => "Read",
        Event::Stat(..) => "Stat",
        Event::Accept(..) => "Accept",
        Event::Recv(..) => "Recv",
        Event::Send(..) => "Send",
        Event::Timer(..) => "Timer",
        Event::Socket(..) => "Socket",
        Event::Connect(..) => "Connect",
        Event::Write(..) => "Write",
        Event::Sync(..) => "Sync",
        Event::Shutdown => "Shutdown",
    }
}
