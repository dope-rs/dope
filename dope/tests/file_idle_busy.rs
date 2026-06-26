#![cfg(target_os = "linux")]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use dope::fiber::Holding;
use dope::fiber::file::Source;
use dope::manifold::Manifold;
use dope::manifold::file::Files;
use dope::runtime::dispatcher::Idle;
use dope::runtime::park::Parker;
use dope::runtime::token::{Epoch, LocalIdx, Token};
use dope::{Drive, DriverConfig, Event, Executor};

const ID: u8 = 7;

fn cfg() -> dope::DriverCfg {
    dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>()
}

#[test]
fn files_reports_busy_while_read_in_flight() {
    let mut exec = Executor::new(cfg()).expect("executor");
    let mut files: Pin<Box<Files<ID, 64>>> = Box::pin(Files::new());
    let pipe = dope::platform::Pipe::new().expect("pipe");

    assert!(
        matches!(Manifold::idle(files.as_ref()), Idle::Park(None)),
        "an idle Files must permit parking"
    );

    {
        let read = Files::read_held(
            Holding::of(files.as_mut()),
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
    }

    assert!(
        matches!(Manifold::idle(files.as_ref()), Idle::Busy),
        "an in-flight (orphaned) read must report Busy so the drain loop cannot exit and free its buffer early"
    );

    // SAFETY: pipe.write_fd() is a live write end owned by `pipe`.
    let _ = unsafe { libc::write(pipe.write_fd(), b"x".as_ptr().cast(), 1) };
    let driver = exec.driver_mut();
    let mut cqe_buf = [dope::Cqe::ZERO; 32];
    let mut released = false;
    for _ in 0..200 {
        if matches!(Manifold::idle(files.as_ref()), Idle::Park(None)) {
            released = true;
            break;
        }
        let _ = Drive::park(driver, Duration::from_millis(5));
        let n = Drive::drain(driver, &mut cqe_buf);
        for cqe in &cqe_buf[..n] {
            if let Ok(ev) = Event::try_from(*cqe) {
                Manifold::dispatch(files.as_mut(), ev, driver);
            }
        }
    }
    assert!(
        released,
        "once the file op completes Files must return to Park so the drain loop can finish"
    );
}
