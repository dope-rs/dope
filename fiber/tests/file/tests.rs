use std::cell::RefCell;
use std::mem;
use std::pin::Pin;
use std::time::Duration;

use dope::Completion;
use dope::io::file::OpenPath;
#[cfg(target_os = "linux")]
use dope::manifold::Manifold;
use dope::manifold::file::{FileManifold, Files};
use dope::runtime::Dispatcher;
#[cfg(target_os = "linux")]
use dope::runtime::Idle;
use dope_fiber::file::{BlockRead, Open, Read, Source, SpliceToPipe, Stat};
use dope_fiber::{Batch, OneShot};
use dope_test::{TempFile, drive, drop_pending, file_exec, pump_events};
use o3::cell::BrandCell;

#[cfg(not(target_os = "linux"))]
use dope::driver::token::{Epoch, SlotIndex, Token, kind};
#[cfg(not(target_os = "linux"))]
use dope_core::backend::Sqe;
#[cfg(not(target_os = "linux"))]
use dope_core::io::fd::FdSlot;

const ID: u8 = 7;
const RDONLY: i32 = dope::io::file::O_RDONLY | dope::io::file::O_CLOEXEC;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Host<'d, 'scope> {
    #[pin]
    #[manifold]
    files: FileManifold<'scope, 'd, ID, 64>,
}

type Sess<'scope, 'd> = dope::runtime::Session<'scope, 'd, Files<'d, ID, 64>>;

fn host<'scope, 'd>(sess: &Sess<'scope, 'd>) -> Pin<Box<BrandCell<'d, Host<'d, 'scope>>>> {
    Box::pin(BrandCell::new(Host {
        files: sess.storage().manifold(),
    }))
}

fn open_ro<'scope, 'd>(
    sess: &mut Sess<'scope, 'd>,
    host: Pin<&BrandCell<'d, Host<'d, 'scope>>>,
    path: &str,
) -> Source<'d> {
    let cpath = OpenPath::new(path).expect("path");
    let open = Open::direct(sess.storage(), cpath, RDONLY);
    drive(sess, host, open).expect("open")
}

fn read_expect<'scope, 'd, K>(
    sess: &mut Sess<'scope, 'd>,
    host: Pin<&BrandCell<'d, Host<'d, 'scope>>>,
    src: &Source<'d, K>,
    payload: &[u8],
) {
    let read = Read::new(sess.storage(), src, vec![0u8; payload.len()], 0);
    let (dst, res) = drive(sess, host, read);
    let n = res.expect("read");
    assert_eq!(n, payload.len());
    assert_eq!(&dst[..n], payload);
}

fn read_owned_expect<'scope, 'd, K>(
    sess: &mut Sess<'scope, 'd>,
    host: Pin<&BrandCell<'d, Host<'d, 'scope>>>,
    src: &Source<'d, K>,
    payload: &[u8],
) {
    let read = BlockRead::new(sess.storage(), src, o3::buffer::Block::new(), 0);
    let (dst, result) = drive(sess, host, read);
    assert_eq!(result.expect("owned read"), payload.len());
    assert_eq!(dst.as_slice(), payload);
}

fn forget_pending<'d, F: dope_fiber::Fiber<'d>>(
    driver: &mut dope::DriverContext<'_, 'd>,
    fiber: F,
    route: u8,
) {
    let mut pending =
        Box::pin(OneShot::new(fiber, route, driver.driver_ref()).expect("ready slot"));
    pending.as_mut().pre_park(driver);
    assert!(!pending.as_ref().is_done());
    mem::forget(pending);
}

fn next_event<'scope, 'd>(sess: &mut Sess<'scope, 'd>) -> dope::Event<'d> {
    let events = RefCell::new(Vec::new());
    pump_events(
        sess,
        |event| events.borrow_mut().push(event),
        || !events.borrow().is_empty(),
    );
    let mut events = events.into_inner();
    assert_eq!(events.len(), 1);
    events.pop().expect("one event")
}

#[test]
fn dropping_pending_read_reclaims_slot() {
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let pipe = dope::io::pipe::Pipe::new().expect("pipe");

        let src = Source::try_from_fd(pipe.read_end()).expect("source");
        let read = Read::new(sess.storage(), &src, vec![0u8; 16], 0);
        let mut driver = sess.driver_access();
        drop_pending(&mut driver, read, u8::MAX);
        let _ = driver.wait(Some(Duration::from_millis(5)));

        let _ = unsafe { libc::write(pipe.write_fd(), b"x".as_ptr().cast(), 1) };
        let seen = RefCell::new(Vec::new());
        pump_events(
            &mut sess,
            |ev| seen.borrow_mut().push(ev),
            || !seen.borrow().is_empty(),
        );
        for ev in seen.into_inner() {
            let (token, mut driver) = sess.token_and_driver();
            Dispatcher::dispatch(host.borrow_pin_mut(token), ev, &mut driver);
        }

        let payload = b"reclaimed-after-cancel";
        let file = TempFile::with("after_cancel", payload);
        let f = dope::io::file::OsFile::open(file.path_str()).expect("open");
        let src = Source::try_from_fd(f.as_fd()).expect("source");
        read_expect(&mut sess, host, &src, payload);
    });
}

#[test]
fn route_awaits_open_then_read() {
    let payload = b"awaited-through-the-io-uring-loop";
    let file = TempFile::with("open_read", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();

        let src = open_ro(&mut sess, host, file.path_str());
        read_expect(&mut sess, host, &src, payload);
        read_owned_expect(&mut sess, host, &src, payload);

        let f = dope::io::file::OsFile::open(file.path_str()).expect("open");
        let raw = Source::try_from_fd(f.as_fd()).expect("source");
        read_expect(&mut sess, host, &raw, payload);
    });
}

#[test]
fn block_read_stops_at_the_fixed_block_boundary() {
    let payload = vec![b'x'; o3::buffer::Block::CAPACITY + 1];
    let file = TempFile::with("owned_block_boundary", &payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let src = open_ro(&mut sess, host, file.path_str());
        let read = BlockRead::new(sess.storage(), &src, o3::buffer::Block::new(), 0);
        let (dst, result) = drive(&mut sess, host, read);
        assert_eq!(
            result.expect("fixed-block read"),
            o3::buffer::Block::CAPACITY
        );
        assert_eq!(dst.as_slice(), &payload[..o3::buffer::Block::CAPACITY]);
    });
}

#[test]
fn block_read_with_full_buffer_completes_without_submission() {
    let file = TempFile::with("owned_already_full", b"not-read");
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let src = open_ro(&mut sess, host, file.path_str());
        let mut buffer = o3::buffer::Block::new();
        let contents = vec![b'x'; o3::buffer::Block::CAPACITY];
        buffer
            .try_extend_from_slice(&contents)
            .expect("fill owned buffer");

        let read = BlockRead::new(sess.storage(), &src, buffer, u64::MAX);
        let (buffer, result) = drive(&mut sess, host, read);

        assert_eq!(result.expect("already full"), contents.len());
        assert_eq!(buffer.as_slice(), contents);
    });
}

#[test]
fn batch_file_completions_wake_the_exact_children() {
    let first = TempFile::with("batch_first", b"first");
    let second = TempFile::with("batch_second", b"second");
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let opens = Batch::from_array([
            Open::direct(
                sess.storage(),
                OpenPath::new(first.path_str()).expect("first path"),
                RDONLY,
            ),
            Open::direct(
                sess.storage(),
                OpenPath::new(second.path_str()).expect("second path"),
                RDONLY,
            ),
        ]);

        let sources = drive(&mut sess, host, opens).collect::<Vec<_>>();
        assert_eq!(sources.len(), 2);
        assert!(sources.into_iter().all(|source| source.is_ok()));
    });
}

#[test]
fn forgotten_file_operations_keep_kernel_backing_alive() {
    let payload = b"forgotten-operation-backing";
    let file = TempFile::with("forgotten", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let path = OpenPath::new(file.path_str()).expect("path");
        let open = Open::direct(sess.storage(), path, RDONLY);
        forget_pending(&mut sess.driver_access(), open, 200);

        let input = dope::io::file::OsFile::open(file.path_str()).expect("input");
        let source = Source::try_from_fd(input.as_fd()).expect("source");
        let read = Read::new(sess.storage(), &source, vec![0; payload.len()], 0);
        forget_pending(&mut sess.driver_access(), read, 201);
        drop(source);
        drop(input);

        let path = OpenPath::new(file.path_str()).expect("path");
        let stat = Stat::path(sess.storage(), path);
        forget_pending(&mut sess.driver_access(), stat, 202);

        let input = dope::io::file::OsFile::open(file.path_str()).expect("input");
        let pipe = dope::io::pipe::Pipe::new().expect("pipe");
        let source = Source::try_from_fd(input.as_fd()).expect("source");
        let sink = Source::try_from_fd(pipe.write_end()).expect("sink");
        let splice = SpliceToPipe::new(sess.storage(), &source, &sink, 0, payload.len() as u32);
        forget_pending(&mut sess.driver_access(), splice, 203);
        drop(source);
        drop(sink);
        drop(input);
        drop(pipe);

        let source = open_ro(&mut sess, host, file.path_str());
        read_expect(&mut sess, host, &source, payload);
    });
}

#[test]
fn route_awaits_path_and_open_file_metadata() {
    let payload = b"async-statx-metadata";
    let file = TempFile::with("stat", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let cpath = OpenPath::new(file.path_str()).expect("path");
        let stat = Stat::path(sess.storage(), cpath);
        let metadata = drive(&mut sess, host, stat).expect("path stat");
        assert!(metadata.is_file(), "{metadata:?}");
        assert_eq!(metadata.len(), payload.len() as u64);

        let source = open_ro(&mut sess, host, file.path_str());
        let stat = Stat::source(sess.storage(), &source);
        let metadata = drive(&mut sess, host, stat).expect("fd stat");
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), payload.len() as u64);
    });
}

#[test]
fn route_awaits_open_enoent_clean_error() {
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let cpath = OpenPath::new("/nonexistent/dope/async/missing/file").expect("path");

        let open = Open::direct(sess.storage(), cpath, RDONLY);
        let err = drive(&mut sess, host, open).expect_err("open should fail");
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
    });
}

#[test]
fn route_awaits_splice_file_to_pipe() {
    let payload = b"async-splice-file-to-pipe";
    let file = TempFile::with("splice", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let f = dope::io::file::OsFile::open(file.path_str()).expect("open");
        let pipe = dope::io::pipe::Pipe::new().expect("pipe");
        let src = Source::try_from_fd(f.as_fd()).expect("source");
        let sink = Source::try_from_fd(pipe.write_end()).expect("sink");

        let splice = SpliceToPipe::new(sess.storage(), &src, &sink, 0, payload.len() as u32);
        let moved = drive(&mut sess, host, splice).expect("splice");
        assert_eq!(moved, payload.len());

        let mut got = vec![0u8; payload.len()];
        let n = unsafe { libc::read(pipe.read_fd(), got.as_mut_ptr().cast(), got.len()) };
        assert_eq!(n, payload.len() as isize);
        assert_eq!(&got, payload);
    });
}

#[test]
fn route_awaits_fixed_file_open_and_read() {
    let payload = b"fixed-file-table-read";
    let file = TempFile::with("fixed", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let cpath = OpenPath::new(file.path_str()).expect("path");
        let (storage, mut driver) = sess.storage_and_driver();
        let fd = storage.alloc_fixed(&mut driver).expect("fixed fd");

        let open = Open::fixed(sess.storage(), cpath, RDONLY, fd);
        let src = drive(&mut sess, host, open).expect("fixed open");
        assert!(src.is_fixed());

        read_expect(&mut sess, host, &src, payload);
    });
}

#[test]
fn canceled_fixed_open_slot_is_reused_immediately() {
    let payload = b"fixed-slot-reuse";
    let file = TempFile::with("fixed_reuse", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let raw_path = OpenPath::new(file.path_str()).expect("path");
        let (storage, mut driver) = sess.storage_and_driver();
        let fd = storage.alloc_fixed(&mut driver).expect("fixed fd");
        let index = fd.index();

        #[cfg(not(target_os = "linux"))]
        {
            use dope::Submission;

            let mut driver = sess.driver_access();
            for item in 0..512 {
                let token = Token::new(ID + 1, SlotIndex::new(item), Epoch::INITIAL);
                driver
                    .push(unsafe { Sqe::write_fd(-1, b"x", 0, token) })
                    .expect("queue unrelated completion");
                if item < 4 {
                    let token = Token::new(ID + 2, SlotIndex::new(item), Epoch::INITIAL);
                    driver
                        .push(unsafe {
                            Sqe::openat_fixed(
                                libc::AT_FDCWD,
                                raw_path.as_ptr(),
                                dope::io::file::O_RDONLY,
                                0,
                                FdSlot::new(index),
                                token,
                            )
                            .expect("fixed open sqe")
                        })
                        .expect("queue same-slot create");
                }
            }
        }

        let open = Open::fixed(
            sess.storage(),
            OpenPath::new(file.path_str()).expect("path"),
            RDONLY,
            fd,
        );
        drop_pending(&mut sess.driver_access(), open, u8::MAX);
        {
            let (token, mut driver) = sess.token_and_driver();
            Dispatcher::pre_park(host.borrow_pin_mut(token), &mut driver);
        }

        #[cfg(not(target_os = "linux"))]
        {
            let fd = unsafe {
                dope_core::io::fd::Fd::from_raw_slot(
                    dope_core::io::fd::FdSlot::new(index),
                    sess.driver(),
                )
            };
            let cpath = OpenPath::new(file.path_str()).expect("path");
            let open = Open::fixed(sess.storage(), cpath, RDONLY, fd);
            let mut driver = sess.driver_access();
            let mut second = Some(Box::pin(
                OneShot::new(open, u8::MAX, driver.driver_ref()).expect("ready slot"),
            ));
            second
                .as_mut()
                .expect("second fixed open")
                .as_mut()
                .pre_park(&mut driver);
            let mut completions = [const { None }; 32];
            let mut seen = 0;
            let mut cancelled = 0;
            while seen < 512 || cancelled < 6 {
                let count = driver.drain(&mut completions);
                assert_ne!(count, 0, "unrelated completions were lost");
                for completion in &mut completions[..count] {
                    let completion = completion.take().expect("completion slot");
                    if completion.result() == -libc::ECANCELED {
                        assert!(completion.route() == ID || completion.route() == ID + 2);
                        assert_eq!(completion.operation(), kind::OPEN);
                        cancelled += 1;
                        if cancelled == 1 {
                            drop(second.take());
                        }
                    } else {
                        let token = completion.token().expect("routed completion");
                        assert_eq!(token.route(), ID + 1);
                        assert_eq!(token.slot().raw(), seen);
                        assert_eq!(completion.operation(), kind::WRITE);
                        assert_eq!(completion.result(), -libc::EBADF);
                        seen += 1;
                    }
                }
            }
            assert_eq!(cancelled, 6);
        }

        let fd = unsafe {
            dope_core::io::fd::Fd::from_raw_slot(
                dope_core::io::fd::FdSlot::new(index),
                sess.driver(),
            )
        };
        let open = Open::fixed(sess.storage(), raw_path, RDONLY, fd);
        let src = drive(&mut sess, host, open).expect("reused fixed open");
        read_expect(&mut sess, host, &src, payload);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn files_returns_idle_after_pending_read_drop() {
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = Box::pin(BrandCell::new(sess.storage().manifold()));
        let pipe = dope::io::pipe::Pipe::new().expect("pipe");
        assert!(
            matches!(
                Manifold::idle(files.as_ref().borrow_pin(sess.token())),
                Idle::Park(None)
            ),
            "an idle Files must permit parking"
        );

        let src = Source::try_from_fd(pipe.read_end()).expect("source");
        let read = Read::new(sess.storage(), &src, vec![0u8; 16], 0);
        drop_pending(&mut sess.driver_access(), read, u8::MAX);
        {
            let (token, mut driver) = sess.token_and_driver();
            Manifold::pre_park(files.as_ref().borrow_pin_mut(token), &mut driver);
        }

        assert!(
            matches!(
                Manifold::idle(files.as_ref().borrow_pin(sess.token())),
                Idle::Park(None)
            ),
            "a canceled read must release its backing before returning"
        );
    });
}

#[cfg(target_os = "linux")]
#[test]
fn canceling_block_read_reclaims_backend_resubmission() {
    let file = TempFile::with("owned_resubmit_cancel", b"x");
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = Box::pin(BrandCell::new(sess.storage().manifold()));
        let input = dope::io::file::OsFile::open(file.path_str()).expect("input");
        let source = Source::try_from_fd(input.as_fd()).expect("source");
        let read = BlockRead::new(sess.storage(), &source, o3::buffer::Block::new(), 0);
        let mut pending = Box::pin(OneShot::new(read, 210, sess.driver()).expect("ready slot"));
        pending.as_mut().pre_park(&mut sess.driver_access());

        let event = next_event(&mut sess);
        let (token, mut driver) = sess.token_and_driver();
        Manifold::dispatch(files.as_ref().borrow_pin_mut(token), event, &mut driver);
        assert!(!pending.as_ref().is_done());

        drop(pending);
        {
            let (token, mut driver) = sess.token_and_driver();
            Manifold::pre_park(files.as_ref().borrow_pin_mut(token), &mut driver);
        }
        assert!(matches!(
            Manifold::idle(files.as_ref().borrow_pin(sess.token())),
            Idle::Park(None)
        ));
    });
}

#[cfg(target_os = "linux")]
#[test]
fn block_read_backend_continuation_reuses_the_operation_token() {
    let file = TempFile::with("owned_resubmit_token", b"short");
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = Box::pin(BrandCell::new(sess.storage().manifold()));
        let input = dope::io::file::OsFile::open(file.path_str()).expect("input");
        let source = Source::try_from_fd(input.as_fd()).expect("source");
        let read = BlockRead::new(sess.storage(), &source, o3::buffer::Block::new(), 0);
        let mut pending = Box::pin(OneShot::new(read, 211, sess.driver()).expect("ready slot"));
        pending.as_mut().pre_park(&mut sess.driver_access());

        let first = next_event(&mut sess);
        let first_token = match first.as_ref() {
            dope::EventRef::Read(token, _) => token,
            _ => panic!("expected first read completion"),
        };
        {
            let (token, mut driver) = sess.token_and_driver();
            Manifold::dispatch(files.as_ref().borrow_pin_mut(token), first, &mut driver);
        }

        let second = next_event(&mut sess);
        let second_token = match second.as_ref() {
            dope::EventRef::Read(token, _) => token,
            _ => panic!("expected terminal read completion"),
        };
        assert_eq!(first_token, second_token);
        {
            let (token, mut driver) = sess.token_and_driver();
            Manifold::dispatch(files.as_ref().borrow_pin_mut(token), second, &mut driver);
        }

        pending.as_mut().pre_park(&mut sess.driver_access());
        let (buffer, result) = pending.as_mut().take_output().expect("owned output");
        assert_eq!(result.expect("owned read"), 5);
        assert_eq!(buffer.as_slice(), b"short");
    });
}
