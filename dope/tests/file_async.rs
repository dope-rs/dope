use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use dope::fiber::Holding;
use dope::fiber::file::Source;
use dope::file::OpenPath;
use dope::manifold::Manifold;
use dope::manifold::file::Files;
use dope::runtime::park::Parker;
use dope::runtime::token::{Epoch, LocalIdx, Token};
use dope::{Drive, DriverConfig, Event, Executor};

const ID: u8 = 7;

fn cfg() -> dope::DriverCfg {
    dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>()
}

fn temp_path(name: &str) -> String {
    let dir = std::env::temp_dir();
    let p = dir.join(format!("dope_file_async_{}_{}", std::process::id(), name));
    p.to_string_lossy().into_owned()
}

fn drive<F: Future>(exec: &mut Executor, host: Holding<'_, Files<ID, 64>>, fut: F) -> F::Output {
    let driver = exec.driver_mut();

    let sentinel = Token::new(ID, LocalIdx::new(63), Epoch::INITIAL);
    let slot = Parker::make_slot(&*driver, sentinel);
    let waker = slot.make_waker();
    let mut cx = Context::from_waker(&waker);

    let mut fut = Box::pin(fut);
    let mut cqe_buf = [dope::Cqe::ZERO; 32];
    let mut wake_buf: Vec<Token> = Vec::new();

    for _ in 0..500 {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
        let _ = Drive::park(driver, Duration::from_millis(20));
        let n = Drive::drain(driver, &mut cqe_buf);
        for cqe in &cqe_buf[..n] {
            if let Ok(ev) = Event::try_from(*cqe) {
                Manifold::dispatch(host.hold(), ev, driver);
            }
        }
        wake_buf.clear();
        Parker::drain(&*driver, &mut wake_buf);
    }
    panic!("future did not complete");
}

// A read dropped mid-flight must reclaim its slot and not corrupt a later read.
#[test]
fn dropping_pending_read_orphans_then_reclaims_slot() {
    let mut exec = Executor::new(cfg()).expect("executor");
    let mut files: Pin<Box<Files<ID, 64>>> = Box::pin(Files::new());
    let pipe = dope::platform::Pipe::new().expect("pipe");
    let host = Holding::of(files.as_mut());

    // Submit a read on an empty pipe (stays Pending), then drop it mid-flight.
    {
        let read = Files::read_held(
            host,
            exec.driver_mut(),
            Source::fd(pipe.read_fd()),
            vec![0u8; 16],
            0,
        );
        let driver = exec.driver_mut();
        let sentinel = Token::new(ID, LocalIdx::new(63), Epoch::INITIAL);
        let slot = Parker::make_slot(&*driver, sentinel);
        let waker = slot.make_waker();
        let mut cx = Context::from_waker(&waker);
        let mut read = Box::pin(read);
        assert!(matches!(read.as_mut().poll(&mut cx), Poll::Pending));
        let _ = Drive::park(driver, Duration::from_millis(5));
    } // read dropped -> slot orphaned

    // Unblock the read so its cancelled op completes and the slot is freed.
    // SAFETY: pipe.write_fd() is a live write end owned by `pipe`.
    let _ = unsafe { libc::write(pipe.write_fd(), b"x".as_ptr().cast(), 1) };
    {
        let driver = exec.driver_mut();
        let mut cqe_buf = [dope::Cqe::ZERO; 32];
        for _ in 0..50 {
            let _ = Drive::park(driver, Duration::from_millis(5));
            let n = Drive::drain(driver, &mut cqe_buf);
            for cqe in &cqe_buf[..n] {
                if let Ok(ev) = Event::try_from(*cqe) {
                    Manifold::dispatch(host.hold(), ev, driver);
                }
            }
        }
    }

    let path = temp_path("after_cancel");
    let payload = b"reclaimed-after-cancel";
    std::fs::write(&path, payload).expect("write temp");
    let f = dope::file::OsFile::open(&path).expect("open");
    let read = Files::read_held(
        host,
        exec.driver_mut(),
        Source::of(&f),
        vec![0u8; payload.len()],
        0,
    );
    let (dst, res) = drive(&mut exec, host, read);
    let n = res.expect("read after cancel");
    assert_eq!(n, payload.len());
    assert_eq!(&dst[..n], payload);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn route_awaits_open_then_read() {
    let path = temp_path("open_read");
    let payload = b"awaited-through-the-io-uring-loop";
    std::fs::write(&path, payload).expect("write temp");

    let mut exec = Executor::new(cfg()).expect("executor");
    let mut files: Pin<Box<Files<ID, 64>>> = Box::pin(Files::new());
    let cpath = OpenPath::new(&path).expect("path");

    let host = Holding::of(files.as_mut());
    let open = Files::open_held(
        host,
        exec.driver_mut(),
        &cpath,
        dope::file::O_RDONLY | dope::file::O_CLOEXEC,
    );
    let src = drive(&mut exec, host, open).expect("open");

    let read = Files::read_held(host, exec.driver_mut(), src, vec![0u8; payload.len()], 0);
    let (dst, res) = drive(&mut exec, host, read);
    let n = res.expect("read");

    assert_eq!(n, payload.len());
    assert_eq!(&dst[..n], payload);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn route_awaits_open_enoent_clean_error() {
    let mut exec = Executor::new(cfg()).expect("executor");
    let mut files: Pin<Box<Files<ID, 64>>> = Box::pin(Files::new());
    let cpath = OpenPath::new("/nonexistent/dope/async/missing/file").expect("path");

    let host = Holding::of(files.as_mut());
    let open = Files::open_held(
        host,
        exec.driver_mut(),
        &cpath,
        dope::file::O_RDONLY | dope::file::O_CLOEXEC,
    );
    let err = drive(&mut exec, host, open).expect_err("open should fail");
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[test]
fn route_awaits_read_via_raw_fd() {
    let path = temp_path("raw_fd");
    let payload = b"raw-fd-read-path";
    std::fs::write(&path, payload).expect("write temp");

    let mut exec = Executor::new(cfg()).expect("executor");
    let mut files: Pin<Box<Files<ID, 64>>> = Box::pin(Files::new());
    let f = dope::file::OsFile::open(&path).expect("open");

    let host = Holding::of(files.as_mut());
    let read = Files::read_held(
        host,
        exec.driver_mut(),
        Source::of(&f),
        vec![0u8; payload.len()],
        0,
    );
    let (dst, res) = drive(&mut exec, host, read);
    let n = res.expect("read");

    assert_eq!(n, payload.len());
    assert_eq!(&dst[..n], payload);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn route_awaits_splice_file_to_pipe() {
    let path = temp_path("splice");
    let payload = b"async-splice-file-to-pipe";
    std::fs::write(&path, payload).expect("write temp");

    let mut exec = Executor::new(cfg()).expect("executor");
    let mut files: Pin<Box<Files<ID, 64>>> = Box::pin(Files::new());
    let f = dope::file::OsFile::open(&path).expect("open");
    let pipe = dope::platform::Pipe::new().expect("pipe");

    let host = Holding::of(files.as_mut());
    let splice = Files::splice_to_pipe_held(
        host,
        exec.driver_mut(),
        Source::of(&f),
        0,
        pipe.write_fd(),
        payload.len() as u32,
    );
    let moved = drive(&mut exec, host, splice).expect("splice");
    assert_eq!(moved, payload.len());

    let mut got = vec![0u8; payload.len()];
    // SAFETY: pipe.read_fd() is a live read end owned by `pipe`; read fills up to got.len() bytes.
    let n = unsafe { libc::read(pipe.read_fd(), got.as_mut_ptr().cast(), got.len()) };
    assert_eq!(n, payload.len() as isize);
    assert_eq!(&got, payload);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn route_awaits_fixed_file_open_and_read() {
    let path = temp_path("fixed");
    let payload = b"fixed-file-table-read";
    std::fs::write(&path, payload).expect("write temp");

    let mut exec = Executor::new(cfg()).expect("executor");
    let mut files: Pin<Box<Files<ID, 64>>> = Box::pin(Files::new());
    let cpath = OpenPath::new(&path).expect("path");

    let slot = Files::<ID, 64>::alloc_fixed_slot(exec.driver_mut(), 1).expect("fixed slot");

    let host = Holding::of(files.as_mut());
    let open = Files::open_fixed_held(
        host,
        exec.driver_mut(),
        &cpath,
        dope::file::O_RDONLY | dope::file::O_CLOEXEC,
        slot,
    );
    let src = drive(&mut exec, host, open).expect("fixed open");
    assert!(matches!(src, Source::Fixed(_)));

    let read = Files::read_held(host, exec.driver_mut(), src, vec![0u8; payload.len()], 0);
    let (dst, res) = drive(&mut exec, host, read);
    let n = res.expect("fixed read");

    assert_eq!(n, payload.len());
    assert_eq!(&dst[..n], payload);
    let _ = std::fs::remove_file(&path);
}
