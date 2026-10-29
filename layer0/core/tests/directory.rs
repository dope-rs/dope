use std::{mem, os::fd, path::Path};

use dope_core::io::fs::{self, open};
use dope_test::alloc::Tracker;
use fd::{AsFd as _, AsRawFd as _};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn copies_unix_path() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};

    use fs::path::CStrExt as _;

    let bytes = b"../\xff/dir";
    let path = Box::try_copy(Path::new(OsStr::from_bytes(bytes))).expect("Unix path");
    assert_eq!(path.to_bytes(), bytes);
}

#[test]
fn reports_nul_position() {
    assert!(matches!(
        fs::Dir::open("bad\0path"),
        Err(fs::open::Error::Path(fs::path::Error::Nul { position: 3 }))
    ));
}

#[test]
fn reports_allocation() {
    for allowed in 0..2 {
        let opened = Tracker::fail_after(allowed, || fs::Dir::open(env!("CARGO_MANIFEST_DIR")));
        assert!(matches!(
            opened,
            Err(fs::open::Error::Allocation(o3::alloc::Error::Exhausted))
        ));
    }
}

#[test]
fn preserves_open_failure() {
    assert!(matches!(
        fs::Dir::open(""),
        Err(fs::open::Error::Directory(fault)) if fault.code().get() == libc::ENOENT
    ));

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    assert!(matches!(
        fs::Dir::open(manifest),
        Err(fs::open::Error::Directory(fault)) if fault.code().get() == libc::ENOTDIR
    ));
}

#[test]
fn owns_cloexec_directory() {
    let (opened, (allocs, _)) = Tracker::usage(|| fs::Dir::open(env!("CARGO_MANIFEST_DIR")));
    let dir = (opened).expect("the package dir must open");
    assert_eq!(allocs, 2);
    let fd = dir.as_fd();
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0);
    assert_eq!(flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC);

    let mut stat = mem::MaybeUninit::<libc::stat>::uninit();
    assert_eq!(unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) }, 0);
    let stat = unsafe { stat.assume_init() };
    assert_eq!(stat.st_mode & libc::S_IFMT, libc::S_IFDIR);
}

#[test]
fn paths_retain_dir() {
    use fs::path;

    let (fd, name) = {
        let opened = fs::Dir::open(env!("CARGO_MANIFEST_DIR"));
        let dir = (opened).expect("required fixture outcome");
        let fd = dir.as_fd().as_raw_fd();
        let name = path::Component::parse_os("Cargo.toml".as_ref());
        let name = (name).expect("required fixture outcome");

        let (name, usage) = Tracker::usage(|| dir.confine_name(name));
        assert_eq!(usage, (1, 11));
        (fd, name.expect("required fixture outcome"))
    };

    let request =
        Tracker::usage(|| fs::OpenRequest::new(name, open::Options::new(open::Access::Read)));
    assert_eq!(request.1, (0, 0));
    let mut stat = mem::MaybeUninit::<libc::stat>::uninit();
    assert_eq!(unsafe { libc::fstat(fd, stat.as_mut_ptr()) }, 0);
    let stat = unsafe { stat.assume_init() };
    assert_eq!(stat.st_mode & libc::S_IFMT, libc::S_IFDIR);
}
