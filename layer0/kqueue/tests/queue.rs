use crate::common::slots::inputs::Layout;
pub mod common;

use std::{
    fs,
    io::Write as _,
    num::NonZeroUsize,
    os::fd::{AsFd as _, AsRawFd as _},
    time,
};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno,
    io::reactor::ready::{self, Poll as _, delivery::Event as _},
    kernel::errno::Source as _,
    table::{self, Files as _, change::Interest as _},
};
use o3::mem::quota;

use crate::common::{
    Budget, Failure,
    slots::Slots,
    socket::{try_pair, try_pairs},
};

type Poll = <Kqueue as ready::Source>::Poll<Slots, Failure>;
type Files = <Kqueue as table::Source>::Files<Slots>;

trait MaybeSend<A> {}
impl<T: ?Sized> MaybeSend<()> for T {}
impl<T: ?Sized + Send> MaybeSend<u8> for T {}

trait MaybeSync<A> {}
impl<T: ?Sized> MaybeSync<()> for T {}
impl<T: ?Sized + Sync> MaybeSync<u8> for T {}

fn confined<T: ?Sized + MaybeSend<A> + MaybeSync<B>, A, B>() {}

const _: fn() = || {
    confined::<Poll, _, _>();
    confined::<dope_sys::io::file::Dir<Kqueue>, _, _>();
};

#[test]
fn owns_cloexec_fd() {
    let queue = Poll::try_new().expect("kqueue creation must succeed");
    // SAFETY: the live queue owns this descriptor; F_GETFD takes no extra argument.
    let flags = unsafe { libc::fcntl(queue.as_fd().as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0);
    assert_ne!(flags & libc::FD_CLOEXEC, 0);
}

#[test]
fn fault_stops_native_work() {
    use std::ops::ControlFlow;

    let mut queue = Poll::try_new().expect("kqueue creation must succeed");
    let mut calls = 0;
    let first = queue.execute(
        || {
            calls += 1;
            Ok::<_, dope_sys::Errno>(7)
        },
        Failure::System,
    );
    assert_eq!(first, ControlFlow::Continue(7));
    assert_eq!(queue.check(), Ok(()));
    let second = queue.execute(
        || {
            calls += 1;
            Err::<(), _>(dope_sys::Errno::BAD_DESCRIPTOR)
        },
        Failure::System,
    );
    assert_eq!(second, ControlFlow::Break(()));
    let third = queue.execute(
        || {
            calls += 1;
            Err::<(), _>(dope_sys::Errno::CANCELED)
        },
        Failure::System,
    );
    assert_eq!(third, ControlFlow::Break(()));
    assert_eq!(calls, 2);
    assert_eq!(
        queue.check(),
        Err(Failure::System(dope_sys::Errno::BAD_DESCRIPTOR))
    );
}

#[test]
fn returns_ready_events() {
    let mut queue = Poll::try_new().expect("kqueue creation must succeed");
    let mut files = Files::try_new(Layout).expect("file table must allocate");
    let mut storage = Poll::storage();
    let source = quota::Ledger::new(1);
    let mut budget = Budget::new(&source);
    assert!(
        queue
            .wait(
                &mut files,
                &mut storage,
                4,
                Some(time::Duration::ZERO),
                &mut budget
            )
            .expect("empty poll must succeed")
            .next()
            .is_none()
    );

    let (handle, mut write) = try_pair().expect("socket pair");
    assert_eq!(dope_sys::Budget::remaining(&budget), 1);
    let accepted = files
        .establish(handle)
        .expect("accepted prefix must have one free row");
    files
        .try_read(accepted.physical())
        .expect("accepted descriptor must be registered")
        .arm(
            libc::EV_ADD | libc::EV_CLEAR,
            NonZeroUsize::new(1).expect("nonzero token"),
        );
    write
        .write_all(b"x")
        .expect("registered descriptor must become ready");
    let mut events = queue
        .wait(
            &mut files,
            &mut storage,
            4,
            Some(time::Duration::ZERO),
            &mut budget,
        )
        .expect("ready poll must succeed");
    let event = events.next().expect("the ready event must be returned");
    assert_eq!(event.filter(), libc::EVFILT_READ);
    assert_eq!(event.error(), None);
    assert_eq!(event.data(), 1);
    assert!(events.next().is_none());
    assert_eq!(dope_sys::Budget::remaining(&budget), 0);
    assert!(
        queue
            .wait(
                &mut files,
                &mut storage,
                4,
                Some(time::Duration::ZERO),
                &mut budget
            )
            .expect("cleared poll must succeed")
            .next()
            .is_none()
    );
}

#[test]
fn rejects_large_timeout() {
    let mut queue = Poll::try_new().expect("kqueue creation must succeed");
    let mut files = Files::try_new(Layout).expect("file table must allocate");
    let mut storage = Poll::storage();
    let source = quota::Ledger::new(0);
    let mut budget = Budget::new(&source);
    assert!(matches!(
        queue.wait(
            &mut files,
            &mut storage,
            1,
            Some(time::Duration::MAX),
            &mut budget,
        ),
        Err(Failure::Timeout(timeout)) if timeout == time::Duration::MAX
    ));
    assert_eq!(queue.check(), Ok(()));
}

#[test]
fn retains_pending_events() {
    let mut queue = Poll::try_new().expect("kqueue creation must succeed");
    let mut files = Files::try_new(Layout).expect("file table must allocate");
    let mut storage = Poll::storage();
    let source = quota::Ledger::new(0);
    let mut budget = Budget::new(&source);
    let mut pairs = try_pairs(&mut queue, &mut files).expect("registered socket pairs");
    for (_, write) in &mut pairs {
        write
            .write_all(b"x")
            .expect("registered descriptor must become ready");
    }
    assert!(
        queue
            .wait(
                &mut files,
                &mut storage,
                0,
                Some(time::Duration::ZERO),
                &mut budget
            )
            .expect("zero delivery budget must retain every pending event")
            .next()
            .is_none()
    );
    let mut ids: Vec<_> = queue
        .wait(
            &mut files,
            &mut storage,
            1,
            Some(time::Duration::ZERO),
            &mut budget,
        )
        .expect("one event must fit the delivery budget")
        .map(|event| event.data())
        .collect();
    assert_eq!(ids.len(), 1);
    let rest: Vec<_> = queue
        .wait(
            &mut files,
            &mut storage,
            usize::MAX,
            Some(time::Duration::ZERO),
            &mut budget,
        )
        .expect("remaining events must fit physical storage")
        .map(|event| event.data())
        .collect();
    assert_eq!(rest.len(), 2);
    ids.extend(rest);
    ids.sort_unstable();
    assert_eq!(ids, [1, 2, 3]);
}

#[test]
fn captures_before_call() {
    let closed = unsafe { libc::close(-1) };
    let fd_fault = Kqueue::try_get()
        .expect("errno slot")
        .expect("failed close");
    Kqueue::clear();
    assert_eq!(Kqueue::try_get(), Ok(None));
    let missing = unsafe { libc::access(c"".as_ptr(), libc::F_OK) };
    let path = Kqueue::try_get()
        .expect("errno slot")
        .expect("failed access");

    assert_eq!(closed, -1);
    assert_eq!(missing, -1);
    assert_eq!(fd_fault, Errno::BAD_DESCRIPTOR);
    assert_eq!(path.code().get(), libc::ENOENT);
}

#[test]
fn flag_updates_keep_status() {
    use dope_sys::{Cloexec, Nonblock, io::fd::Fd};

    let file = fs::File::open("/dev/null").expect("native descriptor");
    Kqueue::set::<{ libc::F_SETFL }, Errno>(file.as_fd(), libc::O_APPEND)
        .expect("initial status flag");
    Kqueue::try_update::<Nonblock>(file.as_fd()).expect("nonblocking status");
    Kqueue::update::<Nonblock>(file.as_fd()).expect("retrying nonblocking status");
    // SAFETY: the command reads scalar status flags from this live descriptor.
    let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    assert!(status >= 0, "native status flags");
    assert_eq!(
        status & (libc::O_APPEND | libc::O_NONBLOCK),
        libc::O_APPEND | libc::O_NONBLOCK,
    );
    <Kqueue as Fd>::remove::<Nonblock>(file.as_fd()).expect("blocking status");
    // SAFETY: the command reads scalar status flags from this live descriptor.
    let status = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    assert!(status >= 0, "native status flags");
    assert_eq!(status & (libc::O_APPEND | libc::O_NONBLOCK), libc::O_APPEND);
    Kqueue::set::<{ libc::F_SETFD }, Errno>(file.as_fd(), 0).expect("clear descriptor flag");
    Kqueue::try_update::<Cloexec>(file.as_fd()).expect("close on exec");
    // SAFETY: the command reads scalar descriptor flags from this live descriptor.
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0, "native descriptor flags");
    assert_eq!(flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC);
}

#[test]
fn mkdir_keeps_failure() {
    use dope_sys::{
        io::file::{File as _, Mode},
        kernel,
    };

    let nonce = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .expect("fixture clock")
        .as_nanos();
    let temp = common::Directory::try_new(&std::env::temp_dir(), "mkdir", nonce)
        .expect("isolated directory");
    let mut path = temp.c_path().to_bytes().to_vec();
    path.extend_from_slice(b"/child");
    let path = std::ffi::CString::new(path).expect("child path");
    Kqueue::mkdir::<Errno>(&path, Mode::new(0o700)).expect("create directory");
    assert!(
        Kqueue::stat::<Errno>(&path)
            .expect("directory metadata")
            .kind()
            .is_dir()
    );
    let failure = Kqueue::mkdir::<kernel::Errno<()>>(&path, Mode::new(0o700));
    Kqueue::clear();
    assert_eq!(failure, Err(Errno::ALREADY_EXISTS.into()));
    fs::remove_dir(temp.path().join("child")).expect("child directory cleanup");
}

#[test]
fn keeps_buffered_entries() {
    use dope_sys::io::file::Dir;

    let nonce = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .expect("fixture clock")
        .as_nanos();
    let temp = common::Directory::try_new(&std::env::temp_dir(), "entries", nonce)
        .expect("isolated directory");
    fs::write(temp.path().join("data"), b"").expect("entry fixture");
    let mut dir = Dir::<Kqueue>::open::<Errno>(temp.c_path()).expect("open directory");
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
