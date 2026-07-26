use std::pin::Pin;

use dope::io::file::OpenPath;
use dope::manifold::file::source::Source;
use dope::manifold::file::{FileManifold, Files};
use dope_fiber::abi::batch::Batch;
use dope_fiber::file::open::Open;
use dope_fiber::file::read::Read;
use dope_fiber::file::stat::Stat;
use dope_test::{TempFile, drive, file_exec};
use o3::cell::BrandCell;

const ID: u8 = 7;
const RDONLY: i32 = dope::io::file::O_RDONLY | dope::io::file::O_CLOEXEC;

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
    source: &Source<'d>,
    payload: &[u8],
) {
    let read = Read::new(sess.storage(), source, vec![0; payload.len()], 0);
    let (buffer, result) = drive(sess, host, read);
    let len = result.expect("read");
    assert_eq!(&buffer[..len], payload);
}

#[test]
fn route_awaits_open_read_and_metadata() {
    let payload = b"awaited-through-the-runtime";
    let file = TempFile::with("open_read", payload);
    file_exec::<ID, 64>().enter(|mut sess| {
        let files = host(&sess);
        let host = files.as_ref();
        let source = open_ro(&mut sess, host, file.path_str());

        read_expect(&mut sess, host, &source, payload);

        let stat = Stat::source(sess.storage(), &source);
        let metadata = drive(&mut sess, host, stat).expect("source stat");
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), payload.len() as u64);

        let path = OpenPath::new(file.path_str()).expect("path");
        let stat = Stat::path(sess.storage(), path);
        let metadata = drive(&mut sess, host, stat).expect("path stat");
        assert_eq!(metadata.len(), payload.len() as u64);
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
