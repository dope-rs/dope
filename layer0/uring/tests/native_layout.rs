pub mod common;

use std::{fs, mem, os::fd::AsFd as _, process, slice, time::Duration};

use dope_sys::{
    Errno,
    io::{
        file::{self, Mode},
        request,
        socket::{Domain, Socket as _, addr},
    },
    kernel::{self, errno::Source as _},
    ring::{Source, enter::Enter},
};
use dope_uring::Uring;

use crate::common::{
    Directory,
    layout::{Aligned, encoded},
};

type Borrowed<'a> = <Uring as file::File>::Request<'a>;
type Offset = file::Offset;
type Timeout = <<Uring as Source>::Enter<'static, ()> as Enter>::Timeout;

#[test]
fn cpu_bounds() {
    use dope_sys::thread::Thread as _;

    let expected: Vec<_> = Uring::current().expect("available CPUs").collect();
    let mut cpus = Uring::current().expect("same thread affinity");
    let mut expected = expected.into_iter();
    let mut front = true;
    while expected.len() != 0 {
        assert_eq!(cpus.len(), expected.len());
        assert_eq!(cpus.size_hint(), (expected.len(), Some(expected.len())));
        let actual = if front { cpus.next() } else { cpus.next_back() };
        let wanted = if front {
            expected.next()
        } else {
            expected.next_back()
        };
        assert_eq!(actual, wanted);
        front = !front;
    }
    assert_eq!(cpus.len(), 0);
    assert_eq!(cpus.size_hint(), (0, Some(0)));
    assert_eq!(cpus.next(), None);
    assert_eq!(cpus.next_back(), None);
    assert_eq!(cpus.next(), None);
}

#[test]
fn mapping_layouts() {
    use dope_sys::memory::Error;

    assert!(matches!(
        Uring::layout::<u8>(0),
        Err(Error::Length {
            element: 1,
            count: 0
        })
    ));
    assert!(matches!(
        Uring::layout::<()>(usize::MAX),
        Err(Error::Length {
            element: 0,
            count: usize::MAX
        })
    ));
    let count = isize::MAX as usize + 1;
    assert!(matches!(
        Uring::layout::<u8>(count),
        Err(Error::Length { element: 1, count: actual }) if actual == count
    ));
    let count = isize::MAX as usize / mem::size_of::<u64>() + 1;
    assert!(matches!(
        Uring::layout::<u64>(count),
        Err(Error::Length { element: 8, count: actual }) if actual == count
    ));
    assert!(matches!(
        Uring::layout::<u64>(usize::MAX),
        Err(Error::Length {
            element: 8,
            count: usize::MAX
        })
    ));
    assert!(matches!(
        Uring::layout::<Aligned>(0),
        Err(Error::Length {
            element: 8192,
            count: 0
        })
    ));
    assert!(matches!(
        Uring::layout::<Aligned>(usize::MAX),
        Err(Error::Length {
            element: 8192,
            count: usize::MAX
        })
    ));
    assert!(matches!(
        Uring::layout::<Aligned>(1),
        Err(Error::Alignment {
            element: 8192,
            supported: 4096
        })
    ));
}

#[test]
fn mapping_bounds() {
    for count in [1, 4096, isize::MAX as usize] {
        assert_eq!(
            Uring::layout::<u8>(count).expect("native mapping").get(),
            count
        );
    }
    for count in [1, 512, isize::MAX as usize / mem::size_of::<u64>()] {
        assert_eq!(
            Uring::layout::<u64>(count).expect("native mapping").get(),
            count * mem::size_of::<u64>()
        );
    }
}

#[test]
fn keeps_wrapper_layout() {
    assert_eq!(mem::size_of::<Option<Borrowed<'static>>>(), 72);
    assert_eq!(mem::align_of::<<Uring as Source>::Enter<'static, ()>>(), 4);
}

#[test]
fn timeout_accepts_duration() {
    let result = Timeout::try_from(Duration::new(1, 999_999_999));
    assert!(result.is_ok());
    let edge = Duration::new(i64::MAX as u64, 999_999_999);
    assert!(Timeout::try_from(edge).is_ok());
}

#[test]
fn timeout_overflow() {
    let value = Duration::from_secs(i64::MAX as u64 + 1);
    let overflow = Timeout::try_from(value)
        .err()
        .expect("out-of-range timeout was accepted");
    assert_eq!(overflow.requested(), value);
}

#[test]
fn open_keeps_paths() {
    use std::{fs::File, os::fd, ptr};

    use request::Request as _;

    let dir = File::open(env!("CARGO_MANIFEST_DIR")).expect("package directory");
    let options = Borrowed::options(file::Open::READ, file::Mode::new(0));
    let path = c"entry";
    let entry = Borrowed::open(fd::AsFd::as_fd(&dir), path, &options);
    let data = encoded(&entry);

    assert_eq!(data[0], 28, "IORING_OP_OPENAT2");
    assert_eq!(data[1], 0);
    assert_eq!(&data[4..8], &fd::AsRawFd::as_raw_fd(&dir).to_ne_bytes());
    assert_eq!(
        &data[8..16],
        &(ptr::from_ref(&options) as u64).to_ne_bytes()
    );
    assert_eq!(&data[16..24], &(path.as_ptr() as u64).to_ne_bytes());
    assert_eq!(&data[24..28], &24_u32.to_ne_bytes(), "open_how size");
    assert_eq!(&data[28..], &[0; 36]);
}

#[test]
fn names_keep_async() {
    use std::{fs::File, os::fd};

    use request::Request as _;

    let dir = File::open(env!("CARGO_MANIFEST_DIR")).expect("package directory");
    let raw = fd::AsRawFd::as_raw_fd(&dir).to_ne_bytes();
    let old = c"old";
    let new = c"new";
    let rename = Borrowed::rename(fd::AsFd::as_fd(&dir), old, new);
    let unlink = Borrowed::unlink(fd::AsFd::as_fd(&dir), old);
    let rename = encoded(&rename);
    let unlink = encoded(&unlink);

    assert_eq!(rename[0], 35, "IORING_OP_RENAMEAT");
    assert_eq!(unlink[0], 36, "IORING_OP_UNLINKAT");
    for entry in [&rename, &unlink] {
        assert_eq!(entry[1], 1 << 4, "IOSQE_ASYNC");
        assert_eq!(&entry[4..8], &raw);
        assert_eq!(&entry[16..24], &(old.as_ptr() as u64).to_ne_bytes());
        assert_eq!(&entry[28..], &[0; 36]);
    }
    assert_eq!(&rename[8..16], &(new.as_ptr() as u64).to_ne_bytes());
    assert_eq!(&rename[24..28], &raw);
    assert_eq!(&unlink[8..16], &[0; 8]);
    assert_eq!(&unlink[24..28], &[0; 4]);
}

#[test]
fn offset_keeps_position() {
    use std::{fs::File, os::fd};

    use o3::buffer::bytes;
    use request::Request as _;

    let file = File::open(env!("CARGO_MANIFEST_DIR")).expect("package directory");
    let input = bytes::Prefix::try_new(b"data".as_slice()).expect("bounded input");
    for offset in [0, i64::MAX as u64] {
        let entry = Borrowed::write(
            fd::AsFd::as_fd(&file),
            input,
            Offset::new(offset).expect("absolute position"),
        );
        let data = encoded(&entry);
        assert_eq!(data[0], 23, "IORING_OP_WRITE");
        assert_eq!(data[1], 1 << 4, "IOSQE_ASYNC");
        assert_eq!(&data[8..16], &offset.to_ne_bytes());
        assert_eq!(&data[24..28], &4_u32.to_ne_bytes());
    }
}

#[test]
fn captures_before_call() {
    let closed = unsafe { libc::close(-1) };
    let fd_fault = Uring::try_get().expect("errno slot").expect("failed close");
    Uring::clear();
    assert_eq!(Uring::try_get(), Ok(None));
    let missing = unsafe { libc::access(c"".as_ptr(), libc::F_OK) };
    let path = Uring::try_get()
        .expect("errno slot")
        .expect("failed access");

    assert_eq!(closed, -1);
    assert_eq!(missing, -1);
    assert_eq!(fd_fault, Errno::BAD_DESCRIPTOR);
    assert_eq!(path.code().get(), libc::ENOENT);
}

#[test]
fn unix_path_extent() {
    let offset = mem::offset_of!(libc::sockaddr_un, sun_path);
    let capacity = mem::size_of::<libc::sockaddr_un>() - offset;
    let maximum = capacity - usize::from(<Uring as addr::Source>::UNIX_TERMINATED);
    let bytes = [b'p'; mem::size_of::<libc::sockaddr_un>()];
    let address = addr::Unix::from_path::<Uring>(&bytes[..maximum]).expect("native path limit");
    let (raw, length) = address.as_addr().parts();
    assert_eq!(
        length.get(),
        offset + maximum + usize::from(maximum < capacity),
    );
    // SAFETY: the borrowed address retains its full initialized native payload.
    let encoded = unsafe { slice::from_raw_parts(raw.cast::<u8>().add(offset), maximum) };
    assert_eq!(encoded, &bytes[..maximum]);
    assert!(matches!(
        addr::Unix::from_path::<Uring>(&bytes[..maximum + 1]),
        Err(addr::PathError::Length { actual, maximum: limit })
            if actual == maximum + 1 && limit == maximum
    ));
}

#[test]
fn unix_abstract_extent() {
    let offset = mem::offset_of!(libc::sockaddr_un, sun_path);
    let maximum = mem::size_of::<libc::sockaddr_un>() - offset - 1;
    let mut bytes = [b'n'; mem::size_of::<libc::sockaddr_un>()];
    bytes[..4].copy_from_slice(&process::id().to_ne_bytes());
    bytes[4] = 0;
    let address = addr::Unix::from_name::<Uring>(&bytes[..maximum]);
    let address = address.expect("native abstract name limit");
    let socket = Uring::datagram::<Errno>(Domain::Unix).expect("abstract datagram socket");
    address
        .as_addr()
        .try_bind::<Uring, Errno>(socket.as_fd())
        .expect("bind binary abstract name");
    let mut storage = addr::Storage::empty();
    let local = addr::Addr::local::<Uring, Errno>(socket.as_fd(), &mut storage)
        .expect("native abstract address");
    let (raw, length) = local.parts();
    assert_eq!(length.get(), offset + 1 + maximum);
    // SAFETY: getsockname admitted this exact initialized address extent.
    let encoded = unsafe { slice::from_raw_parts(raw.cast::<u8>().add(offset), maximum + 1) };
    assert_eq!(encoded[0], 0);
    assert_eq!(&encoded[1..], &bytes[..maximum]);
    assert!(matches!(
        addr::Unix::from_name::<Uring>(&bytes[..maximum + 1]),
        Err(addr::PathError::Length { actual, maximum: limit })
            if actual == maximum + 1 && limit == maximum
    ));
}

#[test]
fn mkdir_keeps_failure() {
    use dope_sys::io::file::File as _;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("fixture clock")
        .as_nanos();
    let temp =
        Directory::try_new(&std::env::temp_dir(), "mkdir", nonce).expect("isolated directory");
    let mut path = temp.c_path().to_bytes().to_vec();
    path.extend_from_slice(b"/child");
    let path = std::ffi::CString::new(path).expect("child path");
    Uring::mkdir::<Errno>(&path, Mode::new(0o700)).expect("create directory");
    assert!(
        Uring::stat::<Errno>(&path)
            .expect("directory metadata")
            .kind()
            .is_dir()
    );
    let failure = Uring::mkdir::<kernel::Errno<()>>(&path, Mode::new(0o700));
    Uring::clear();
    assert_eq!(failure, Err(kernel::Errno::from(Errno::ALREADY_EXISTS)));
    fs::remove_dir(temp.path().join("child")).expect("child directory cleanup");
}

#[test]
fn keeps_buffered_entries() {
    use dope_sys::io::file::Dir;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("fixture clock")
        .as_nanos();
    let temp = Directory::try_new(&std::env::temp_dir(), "directory_entries", nonce)
        .expect("isolated directory");
    fs::write(temp.path().join("data"), b"").expect("entry fixture");
    let mut dir = Dir::<Uring>::open::<Errno>(temp.c_path()).expect("open directory");
    let first = {
        let (_, mut entries) = dir.entries();
        entries
            .try_next::<Errno>()
            .expect("first read")
            .expect("first entry")
            .name
            .to_owned()
    };
    let mut names = vec![first];
    let (_, mut entries) = dir.entries();
    while let Some(entry) = entries.try_next::<Errno>().expect("remaining entries") {
        if let Some(kind) = entry.kind {
            assert_eq!(kind.is_regular(), entry.name == c"data");
        }
        names.push(entry.name.to_owned());
    }
    names.sort_unstable();
    assert_eq!(names, [c".", c"..", c"data"]);
    drop(dir);
    fs::remove_file(temp.path().join("data")).expect("entry fixture cleanup");
}
