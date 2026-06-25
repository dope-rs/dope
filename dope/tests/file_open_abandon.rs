// Isolated in its own test binary: the fd-leak check reads the process-global
// /proc/self/fd, which would be perturbed by other tests running in parallel.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use dope::fiber::Holding;
use dope::file::OpenPath;
use dope::manifold::Manifold;
use dope::manifold::file::Files;
use dope::runtime::park::Parker;
use dope::runtime::token::{Epoch, LocalIdx, Token};
use dope::{Drive, DriverConfig, Event, Executor};

const ID: u8 = 7;

// A completed-but-abandoned open must not leak its fd.
#[test]
fn abandoned_completed_open_closes_fd() {
    let path = std::env::temp_dir().join(format!("dope_abandon_open_{}", std::process::id()));
    std::fs::write(&path, b"x").expect("write temp");

    let mut exec = Executor::new(dope::DriverCfg::for_profile::<
        dope::runtime::profile::Throughput,
    >())
    .expect("executor");
    let mut files: Pin<Box<Files<ID, 64>>> = Box::pin(Files::new());
    let cpath = OpenPath::new(path.to_str().unwrap()).expect("path");
    let host = Holding::of(files.as_mut());

    let fd_count = || std::fs::read_dir("/proc/self/fd").expect("procfs").count();
    let before = fd_count();

    {
        let open = Files::open_held(
            host,
            exec.driver_mut(),
            &cpath,
            dope::file::O_RDONLY | dope::file::O_CLOEXEC,
        );
        let driver = exec.driver_mut();
        let sentinel = Token::new(ID, LocalIdx::new(63), Epoch::INITIAL);
        let slot = Parker::make_slot(&*driver, sentinel);
        let waker = slot.make_waker();
        let mut cx = Context::from_waker(&waker);
        let mut open = Box::pin(open);
        assert!(matches!(open.as_mut().poll(&mut cx), Poll::Pending));

        // Drive until the open settles, without polling `open` again.
        let mut cqe_buf = [dope::Cqe::ZERO; 32];
        for _ in 0..200 {
            if fd_count() > before {
                break;
            }
            let _ = Drive::park(driver, Duration::from_millis(5));
            let n = Drive::drain(driver, &mut cqe_buf);
            for cqe in &cqe_buf[..n] {
                if let Ok(ev) = Event::try_from(*cqe) {
                    Manifold::dispatch(host.hold(), ev, driver);
                }
            }
        }
        assert!(fd_count() > before, "open did not produce an fd to leak");
    } // open dropped -> fd closed

    assert_eq!(fd_count(), before, "abandoned open leaked its fd");
    let _ = std::fs::remove_file(&path);
}
