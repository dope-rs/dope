use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::time::Duration;

use dope_core::driver::completion::Completion;
use dope_core::driver::submission::Submission;

use dope_core::backend::Sqe;
use dope_core::driver::DriverContext;
use dope_core::driver::token::{Epoch, SlotIndex, Token};
use dope_core::io::file::{self, OpenPath, OsFile};
use dope_core::io::pipe::Pipe;
use dope_core::io::{Event, EventRef, OpenEvent, ReadEvent, SpliceEvent, WriteEvent};
use dope_test::{TempFile, with_driver};

fn tok(n: u32) -> Token {
    Token::new(7, SlotIndex::new(n), Epoch::INITIAL)
}

fn event_token(ev: &Event) -> Token {
    match ev.as_ref() {
        EventRef::Accept(t, ..)
        | EventRef::Recv(t, ..)
        | EventRef::Send(t, ..)
        | EventRef::Timer(t)
        | EventRef::Socket(t, ..)
        | EventRef::Connect(t, ..)
        | EventRef::Write(t, ..)
        | EventRef::Sync(t, ..)
        | EventRef::Open(t, ..)
        | EventRef::Read(t, ..)
        | EventRef::Splice(t, ..)
        | EventRef::Stat(t, ..) => t,
        EventRef::Shutdown => panic!("unexpected shutdown completion"),
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

fn loopback_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let sender = TcpStream::connect(addr).expect("connect");
    let (receiver, _) = listener.accept().expect("accept");
    receiver
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("timeout");
    (sender, receiver)
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
            let t = tok(i as u32 + 1);
            driver
                .push(unsafe { f.read_at(&mut dst, offset, t) })
                .expect("push read");

            let event = drive_until(&mut driver, t);
            match (event.as_ref(), want) {
                (EventRef::Read(_, ReadEvent::Read(n)), Some(len)) => {
                    assert_eq!(*n as usize, len);
                    assert_eq!(&dst[..*n as usize], payload);
                }
                (EventRef::Read(_, ReadEvent::Eof), None) => {}
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
            .push(unsafe { path.open_at(file::O_RDONLY | file::O_CLOEXEC, t) })
            .expect("push open");

        let event = drive_until(&mut driver, t);
        match event.as_ref() {
            EventRef::Open(_, OpenEvent::Failed(errno)) => {
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
            .push(unsafe { cpath.open_at(file::O_RDONLY | file::O_CLOEXEC, to) })
            .expect("push open");

        let event = drive_until(&mut driver, to);
        let fd = match event.as_ref() {
            EventRef::Open(_, OpenEvent::Opened(fd)) => {
                assert!(*fd >= 0);
                *fd
            }
            _ => panic!("expected open success, got {}", variant(&event)),
        };

        let mut dst = vec![0u8; payload.len()];
        let tr = tok(11);
        driver
            .push(unsafe { Sqe::read(fd, &mut dst, 0, tr) })
            .expect("push read");

        let event = drive_until(&mut driver, tr);
        let n = match event.as_ref() {
            EventRef::Read(_, ReadEvent::Read(n)) => *n as usize,
            _ => panic!("expected read, got {}", variant(&event)),
        };
        assert_eq!(&dst[..n], payload);

        unsafe { libc::close(fd) };
    });
}

#[test]
fn splice_file_to_socket_zero_copy() {
    let payload = b"zero-copy-splice-payload-over-loopback";
    let tmp = TempFile::with("splice", payload);
    let (sender, mut receiver) = loopback_pair();

    with_driver(|mut driver| {
        let f = OsFile::open(tmp.path_str()).expect("open");
        let pipe = Pipe::new().expect("pipe");

        let t_in = tok(20);
        driver
            .push(unsafe { f.splice_to_pipe(0, pipe.write_fd(), payload.len() as u32, t_in) })
            .expect("push splice in");

        let event = drive_until(&mut driver, t_in);
        let moved_in = match event.as_ref() {
            EventRef::Splice(_, SpliceEvent::Moved(n)) => *n as usize,
            _ => panic!("expected splice into pipe, got {}", variant(&event)),
        };
        assert_eq!(moved_in, payload.len());

        let t_out = tok(21);
        let sock_fd = sender.as_raw_fd();
        driver
            .push(unsafe {
                Sqe::splice_raw(pipe.read_fd(), -1, sock_fd, -1, moved_in as u32, 0, t_out)
            })
            .expect("push splice out");

        let event = drive_until(&mut driver, t_out);
        let moved_out = match event.as_ref() {
            EventRef::Splice(_, SpliceEvent::Moved(n)) => *n as usize,
            _ => panic!("expected splice to socket, got {}", variant(&event)),
        };
        assert_eq!(moved_out, payload.len());

        let mut got = vec![0u8; payload.len()];
        receiver.read_exact(&mut got).expect("read socket");
        assert_eq!(&got, payload);

        drop(sender);
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
            .push(unsafe { Sqe::write_fd(f.fd(), payload, 0, t) })
            .expect("push write");

        let event = drive_until(&mut driver, t);
        match event.as_ref() {
            EventRef::Write(_, WriteEvent::Wrote(n)) => assert_eq!(*n as usize, payload.len()),
            _ => panic!("write did not complete: {}", variant(&event)),
        }

        drop(f);
        let back = std::fs::read(tmp.path_str()).expect("read back");
        assert_eq!(&back, payload);
    });
}

fn variant(ev: &Event) -> &'static str {
    match ev.as_ref() {
        EventRef::Open(..) => "Open",
        EventRef::Read(..) => "Read",
        EventRef::Splice(..) => "Splice",
        EventRef::Stat(..) => "Stat",
        EventRef::Accept(..) => "Accept",
        EventRef::Recv(..) => "Recv",
        EventRef::Send(..) => "Send",
        EventRef::Timer(..) => "Timer",
        EventRef::Socket(..) => "Socket",
        EventRef::Connect(..) => "Connect",
        EventRef::Write(..) => "Write",
        EventRef::Sync(..) => "Sync",
        EventRef::Shutdown => "Shutdown",
    }
}
