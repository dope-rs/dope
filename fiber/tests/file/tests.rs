use std::fs::File;
use std::mem::size_of;
use std::os::fd::OwnedFd;
use std::pin::Pin;

use dope::io::file::OpenPath;
use dope::manifold::file::source::Source;
use dope::manifold::file::{FileManifold, Files};
use dope_fiber::abi::batch::Batch;
use dope_fiber::file::open::Open;
use dope_fiber::file::read_exact::ReadExact;
use dope_fiber::file::stat::Stat;
use dope_test::{TempFile, allocations_during, drive, file_exec};
use o3::cell::BrandCell;

const ID: u8 = 7;
const RDONLY: i32 = dope::io::file::O_RDONLY | dope::io::file::O_CLOEXEC;

#[test]
fn source_is_one_fd_and_heap_free() {
    assert_eq!(size_of::<Source<'static>>(), size_of::<OwnedFd>());

    let file = TempFile::with("source_layout", b"x");
    let fd: OwnedFd = File::open(file.path()).expect("open").into();
    let mut source = None;
    let allocations = allocations_during(|| source = Some(Source::owned(fd)));
    assert_eq!(allocations, (0, 0));

    let source = source.expect("source");
    let mut duplicate = None;
    let allocations =
        allocations_during(|| duplicate = Some(source.try_clone().expect("duplicate")));
    assert_eq!(allocations, (0, 0));
    drop(duplicate);
    drop(source);
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Host<'d, 'scope> {
    #[pin]
    #[manifold]
    files: FileManifold<'scope, 'd, ID, 64>,
}

type Sess<'scope, 'd> = dope::runtime::executor::Session<'scope, 'd, Files<'d, ID, 64>>;

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
    let path = OpenPath::new(path).expect("path");
    drive(sess, host, Open::direct(sess.storage(), path, RDONLY)).expect("open")
}

fn read_expect<'scope, 'd>(
    sess: &mut Sess<'scope, 'd>,
    host: Pin<&BrandCell<'d, Host<'d, 'scope>>>,
    source: Source<'d>,
    payload: &[u8],
) -> Source<'d> {
    let read = ReadExact::new(
        sess.storage(),
        source,
        payload.len().try_into().expect("payload length"),
        0,
    );
    let (source, buffer, result) = drive(sess, host, read);
    result.expect("read");
    assert_eq!(buffer, payload);
    source
}

#[test]
fn route_awaits_open_read_and_metadata() {
    let payload = b"awaited-through-the-runtime";
    let file = TempFile::with("open_read", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let source = open_ro(&mut sess, host, file.path_str());

        let source = read_expect(&mut sess, host, source, payload);

        let stat = Stat::source(sess.storage(), source);
        let (_source, metadata) = drive(&mut sess, host, stat);
        let metadata = metadata.expect("source stat");
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), payload.len() as u64);

        let path = OpenPath::new(file.path_str()).expect("path");
        let stat = Stat::path(sess.storage(), path);
        let metadata = drive(&mut sess, host, stat).expect("path stat");
        assert_eq!(metadata.len(), payload.len() as u64);
    });
}

#[test]
fn short_exact_read_preserves_only_initialized_prefix() {
    let payload = b"short";
    let file = TempFile::with("short_read", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let source = open_ro(&mut sess, host, file.path_str());
        let mut read = None;
        let allocations =
            allocations_during(|| read = Some(ReadExact::new(sess.storage(), source, 64, 0)));
        assert_eq!(allocations, (1, 64));

        let (_source, buffer, result) = drive(&mut sess, host, read.expect("read"));
        assert_eq!(
            result.expect_err("short read").kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert_eq!(buffer, payload);
    });
}

#[test]
fn batch_open_wakes_each_operation() {
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
fn open_missing_path_reports_enoent() {
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let path = OpenPath::new("/nonexistent/dope/async/missing/file").expect("path");
        let open = Open::direct(sess.storage(), path, RDONLY);
        let error = drive(&mut sess, host, open).expect_err("open should fail");
        assert_eq!(error.raw_os_error(), Some(libc::ENOENT));
    });
}
