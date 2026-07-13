use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::time::Duration;

use dope::file::{self, OpenPath, OsFile};
use dope::platform::Pipe;
use dope::runtime::token::{Epoch, LocalIdx, Token};
use dope::sqe::Sqe;
use dope::{Drive, DriverConfig, Event, Executor, OpenEvent, ReadEvent, SpliceEvent};

fn cfg() -> dope::DriverCfg {
    dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>()
}

fn tok(n: u32) -> Token {
    Token::new(7, LocalIdx::new(n), Epoch::INITIAL)
}

fn drive_until(sess: &mut dope::Session<'_>, want: Token) -> Event {
    let driver = sess.driver();
    let mut buf = [dope::Cqe::ZERO; 32];
    for _ in 0..500 {
        let _ = driver.park(Duration::from_millis(20));
        let n = driver.drain(&mut buf);
        for cqe in &buf[..n] {
            let Ok(ev) = Event::try_from(*cqe) else {
                continue;
            };
            if matches!(
                &ev,
                Event::Open(t, _) | Event::Read(t, _) | Event::Splice(t, _)
                    if t.key() == want.key() && t.route() == want.route()
            ) {
                return ev;
            }
        }
    }
    panic!("no completion for token");
}

fn temp_path(name: &str) -> String {
    let dir = std::env::temp_dir();
    let p = dir.join(format!("dope_file_io_{}_{}", std::process::id(), name));
    p.to_string_lossy().into_owned()
}

#[test]
fn async_read_returns_file_bytes() {
    let path = temp_path("read");
    let payload = b"the quick brown fox jumps over the lazy dog";
    std::fs::write(&path, payload).expect("write temp");

    let exec = Executor::new(cfg()).expect("executor");
    let mut sess = exec.enter();
    let f = OsFile::open(&path).expect("open");
    assert_eq!(f.len().expect("len"), payload.len() as u64);

    let mut dst = vec![0u8; payload.len()];
    let t = tok(1);
    sess.driver()
        .push(file::read_at(&f, &mut dst, 0, t))
        .expect("push read");

    let read_len = match drive_until(&mut sess, t) {
        Event::Read(_, ReadEvent::Read(n)) => n as usize,
        other => panic!("unexpected event variant: {}", variant(&other)),
    };

    assert_eq!(read_len, payload.len());
    assert_eq!(&dst[..read_len], payload);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn async_read_eof_at_end_of_file() {
    let path = temp_path("eof");
    std::fs::write(&path, b"abc").expect("write temp");

    let exec = Executor::new(cfg()).expect("executor");
    let mut sess = exec.enter();
    let f = OsFile::open(&path).expect("open");

    let mut dst = vec![0u8; 16];
    let t = tok(2);
    sess.driver()
        .push(file::read_at(&f, &mut dst, 3, t))
        .expect("push read");

    match drive_until(&mut sess, t) {
        Event::Read(_, ReadEvent::Eof) => {}
        other => panic!("expected EOF, got {}", variant(&other)),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn async_read_short_when_buffer_exceeds_file() {
    let path = temp_path("short");
    let payload = b"tiny";
    std::fs::write(&path, payload).expect("write temp");

    let exec = Executor::new(cfg()).expect("executor");
    let mut sess = exec.enter();
    let f = OsFile::open(&path).expect("open");

    let mut dst = vec![0u8; 4096];
    let t = tok(4);
    sess.driver()
        .push(file::read_at(&f, &mut dst, 0, t))
        .expect("push read");

    let n = match drive_until(&mut sess, t) {
        Event::Read(_, ReadEvent::Read(n)) => n as usize,
        other => panic!("expected short read, got {}", variant(&other)),
    };
    assert_eq!(n, payload.len());
    assert_eq!(&dst[..n], payload);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn async_open_missing_path_reports_enoent() {
    let exec = Executor::new(cfg()).expect("executor");
    let mut sess = exec.enter();
    let path = OpenPath::new("/nonexistent/dope/definitely/missing/file").expect("path");
    let t = tok(3);
    sess.driver()
        .push(file::open_at(&path, file::O_RDONLY | file::O_CLOEXEC, t))
        .expect("push open");

    match drive_until(&mut sess, t) {
        Event::Open(_, OpenEvent::Failed(errno)) => {
            assert_eq!(errno, libc::ENOENT);
        }
        other => panic!("expected open failure, got {}", variant(&other)),
    }
}

#[test]
fn async_open_then_read_via_returned_fd() {
    let path = temp_path("open_read");
    let payload = b"opened-by-io-uring";
    std::fs::write(&path, payload).expect("write temp");

    let exec = Executor::new(cfg()).expect("executor");
    let mut sess = exec.enter();
    let cpath = OpenPath::new(&path).expect("path");
    let to = tok(10);
    sess.driver()
        .push(file::open_at(&cpath, file::O_RDONLY | file::O_CLOEXEC, to))
        .expect("push open");

    let fd = match drive_until(&mut sess, to) {
        Event::Open(_, OpenEvent::Opened(fd)) => {
            assert!(fd >= 0);
            fd
        }
        other => panic!("expected open success, got {}", variant(&other)),
    };

    let mut dst = vec![0u8; payload.len()];
    let tr = tok(11);
    sess.driver()
        .push(file::read_fd(fd, &mut dst, 0, tr))
        .expect("push read");

    let n = match drive_until(&mut sess, tr) {
        Event::Read(_, ReadEvent::Read(n)) => n as usize,
        other => panic!("expected read, got {}", variant(&other)),
    };
    assert_eq!(&dst[..n], payload);

    // SAFETY: fd is the direct file descriptor returned by the openat completion above.
    unsafe { libc::close(fd) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn splice_file_to_socket_zero_copy() {
    let path = temp_path("splice");
    let payload = b"zero-copy-splice-payload-over-loopback";
    std::fs::write(&path, payload).expect("write temp");

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let sender = TcpStream::connect(addr).expect("connect");
    let (mut receiver, _) = listener.accept().expect("accept");
    receiver
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("timeout");

    let exec = Executor::new(cfg()).expect("executor");
    let mut sess = exec.enter();
    let f = OsFile::open(&path).expect("open");
    let pipe = Pipe::new().expect("pipe");

    let t_in = tok(20);
    sess.driver()
        .push(file::splice_file_to_pipe(
            &f,
            0,
            pipe.write_fd(),
            payload.len() as u32,
            t_in,
        ))
        .expect("push splice in");

    let moved_in = match drive_until(&mut sess, t_in) {
        Event::Splice(_, SpliceEvent::Moved(n)) => n as usize,
        other => panic!("expected splice into pipe, got {}", variant(&other)),
    };
    assert_eq!(moved_in, payload.len());

    let t_out = tok(21);
    let sock_fd = sender.as_raw_fd();
    sess.driver()
        .push(Sqe::splice_raw(
            pipe.read_fd(),
            -1,
            sock_fd,
            -1,
            moved_in as u32,
            0,
            t_out,
        ))
        .expect("push splice out");

    let moved_out = match drive_until(&mut sess, t_out) {
        Event::Splice(_, SpliceEvent::Moved(n)) => n as usize,
        other => panic!("expected splice to socket, got {}", variant(&other)),
    };
    assert_eq!(moved_out, payload.len());

    let mut got = vec![0u8; payload.len()];
    receiver.read_exact(&mut got).expect("read socket");
    assert_eq!(&got, payload);

    drop(sender);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn write_path_still_works() {
    let path = temp_path("write");
    let f = OsFile::create(&path).expect("create");
    let payload = b"write-path-intact";

    let exec = Executor::new(cfg()).expect("executor");
    let sess = exec.enter();
    let t = tok(30);
    sess.driver()
        .push(Sqe::write_fd(f.fd(), payload, 0, t))
        .expect("push write");

    let driver = sess.driver();
    let mut buf = [dope::Cqe::ZERO; 32];
    let mut wrote = None;
    'outer: for _ in 0..500 {
        let _ = driver.park(Duration::from_millis(20));
        let n = driver.drain(&mut buf);
        for cqe in &buf[..n] {
            if let Ok(Event::Write(et, ev)) = Event::try_from(*cqe)
                && et.key() == t.key()
                && et.route() == t.route()
            {
                wrote = Some(ev);
                break 'outer;
            }
        }
    }
    match wrote {
        Some(dope::WriteEvent::Wrote(n)) => assert_eq!(n as usize, payload.len()),
        _ => panic!("write did not complete"),
    }

    drop(f);
    let back = std::fs::read(&path).expect("read back");
    assert_eq!(&back, payload);
    let _ = std::fs::remove_file(&path);
}

fn variant(ev: &Event) -> &'static str {
    match ev {
        Event::Open(..) => "Open",
        Event::Read(..) => "Read",
        Event::Splice(..) => "Splice",
        Event::Accept(..) => "Accept",
        Event::Recv(..) => "Recv",
        Event::Send(..) => "Send",
        Event::Timer(..) => "Timer",
        Event::Socket(..) => "Socket",
        Event::Connect(..) => "Connect",
        Event::Write(..) => "Write",
        Event::Sync(..) => "Sync",
    }
}
