use std::cell::RefCell;

use dope::io::file::OpenPath;
use dope::manifold::Manifold;
use dope_fiber::OneShot;
use dope_fiber::file::Open;
use dope_test::{TempFile, file_exec, pump_events};
use o3::cell::BrandCell;

const ID: u8 = 7;

#[test]
fn abandoned_completed_open_closes_fd() {
    let tmp = TempFile::with("abandon_open", b"x");

    file_exec::<ID, 64>().enter(|mut sess| {
        let files = Box::pin(BrandCell::new(sess.storage().manifold()));
        let cpath = OpenPath::new(tmp.path_str()).expect("path");
        let host = files.as_ref();
        let driver = sess.driver();

        let fd_count = || std::fs::read_dir("/dev/fd").expect("fd dir").count();
        let before = fd_count();

        {
            let open = Open::direct(
                sess.storage(),
                cpath,
                dope::io::file::O_RDONLY | dope::io::file::O_CLOEXEC,
            );
            let mut open = std::pin::pin!(OneShot::new(open, u8::MAX, driver));
            open.as_mut().pre_park(&mut sess.driver_access());
            assert!(!open.as_ref().is_done());

            let pending = RefCell::new(Vec::new());
            for _ in 0..200 {
                if fd_count() > before {
                    break;
                }
                if !pump_events(
                    &mut sess,
                    |ev| pending.borrow_mut().push(ev),
                    || !pending.borrow().is_empty(),
                ) {
                    break;
                }
                for ev in pending.borrow_mut().drain(..) {
                    let (token, mut access) = sess.token_and_driver();
                    Manifold::dispatch(host.as_ref().borrow_pin_mut(token), ev, &mut access);
                }
            }
            assert!(fd_count() > before, "open did not produce an fd to leak");
        }

        assert_eq!(fd_count(), before, "abandoned open leaked its fd");
    });
}
