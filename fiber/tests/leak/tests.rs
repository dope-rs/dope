use std::{path::Path, sync::Mutex, task::Poll};

use dope::core::io::fs::{Directory, Native, OpenPath};
use dope_fiber::{abi::Fiber, file::OpenRegular};
use dope_test::{fibers, file, scenario, scenario::rt::Runtime};

const ID: u8 = 7;
static FD_TESTS: Mutex<()> = Mutex::new(());

fn confined(path: &Path) -> (Directory, OpenPath) {
    let parent = path.parent().expect("file parent");
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .expect("utf-8 file name");
    let directory = Directory::open(parent).expect("directory capability");
    let path = directory.relative(name).expect("relative file path");
    (directory, path)
}

struct YieldTurn(bool);

impl<'d> Fiber<'d> for YieldTurn {
    type Output = ();

    fn poll(call: dope_fiber::context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (mut self_, cx) = call.into_parts();
        if self_.0 {
            return Poll::Ready(());
        }
        self_.0 = true;
        cx.wake();
        Poll::Pending
    }
}

#[pin_project::pin_project]
struct AbandonAfterWake<F, O> {
    #[pin]
    fiber: Option<F>,
    observe: Option<O>,
    started: bool,
}

impl<F, O> AbandonAfterWake<F, O> {
    fn new(fiber: F, observe: O) -> Self {
        Self {
            fiber: Some(fiber),
            observe: Some(observe),
            started: false,
        }
    }
}

impl<'d, F, O> Fiber<'d> for AbandonAfterWake<F, O>
where
    F: Fiber<'d>,
    O: FnOnce() -> bool,
{
    type Output = bool;

    fn poll(call: dope_fiber::context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        if !*this.started {
            let fiber = this.fiber.as_mut().as_pin_mut().unwrap();
            let Some(poll) = cx.as_mut().try_poll(fiber) else {
                return Poll::Pending;
            };
            assert!(poll.is_pending());
            *this.started = true;
            return Poll::Pending;
        }

        let observe = this.observe.take().expect("completion observer");
        let observed = observe();
        this.fiber.set(None);
        Poll::Ready(observed)
    }
}

#[test]
fn abandoned_completed_open_closes_fd() {
    let _serial = FD_TESTS.lock().expect("fd test lock");
    let tmp = file::File::with("abandon_open", b"x");

    Runtime::throughput()
        .files::<ID, 64, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            let (_directory, cpath) = confined(tmp.path());

            let fd_count = || std::fs::read_dir("/dev/fd").expect("fd dir").count();
            let before = fd_count();

            let observed = sess
                .with_app(scenario::ManifoldHost::new(files.manifold()), |mut app| {
                    let access = app.client(|host| host.manifold());
                    let open = OpenRegular::new(access, cpath);
                    fibers::TEST.drive(
                        &mut app,
                        AbandonAfterWake::new(open, || fd_count() > before),
                    )
                })
                .expect("application teardown");

            assert!(observed, "open did not produce an fd to leak");
            assert_eq!(fd_count(), before, "abandoned open leaked its fd");
        })
        .expect("file storage");
}

#[test]
fn abandoned_pending_open_releases_its_slot() {
    let _serial = FD_TESTS.lock().expect("fd test lock");
    let tmp = file::File::with("cancel_pending_open", b"x");

    Runtime::throughput()
        .files::<ID, 1, Native>()
        .try_enter(|mut sess| {
            let files = sess.storage();
            sess.with_app(scenario::ManifoldHost::new(files.manifold()), |mut app| {
                let access = app.client(|host| host.manifold());
                let (_directory, path) = confined(tmp.path());
                let open = OpenRegular::new(access, path);
                fibers::TEST.drive(&mut app, fibers::TEST.cancel_after_poll(open));
                fibers::TEST.drive(&mut app, YieldTurn(false));

                let reopened = OpenRegular::new(
                    access,
                    _directory
                        .relative(
                            tmp.path()
                                .file_name()
                                .and_then(std::ffi::OsStr::to_str)
                                .expect("utf-8 file name"),
                        )
                        .expect("relative file path"),
                );
                let source = fibers::TEST
                    .drive(&mut app, reopened)
                    .expect("cancelled slot must be reusable");
                drop(source);
            })
            .expect("application teardown");
        })
        .expect("file storage");
}
