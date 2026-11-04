pub mod common;

use std::{
    env,
    ffi::OsStr,
    iter, mem, net,
    num::NonZeroUsize,
    os::fd::{AsFd as _, AsRawFd as _},
    process::Command,
    ptr,
};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno,
    engine::admit,
    io::{
        reactor::{
            complete::{self, Cancellation as _, Queue as _},
            ready::{self, Poll as _},
            retry::{self, Entry as _, Occupied as _, State as _, raw::Write as _},
        },
        socket::{self, Prepare as _, Socket as _, msg, raw::Open as _},
    },
    table::{
        Files as _,
        change::{self, Changes as _, Id as _},
    },
};
use o3::buffer::bytes;

use crate::common::{
    Failure,
    cancel::{
        Context,
        write::{Blocked, Outcome, Write},
    },
    slots::{Slots, inputs::Physical},
};

type Poll = <Kqueue as ready::Source>::Poll<Slots, Failure>;
type Pending = <Kqueue as retry::Source>::Pending<Context>;
type File = <Pending as retry::Entry>::File;
type Retry<'a> = <Kqueue as retry::Source>::Retry<'a, Context, Write, Poll>;

#[test]
fn adopted_child_send_keeps_error() {
    use retry::raw::Send as _;

    let name = "adopted_child_send_keeps_error";
    if env::var_os("O3_TEST_CHILD").as_deref() != Some(OsStr::new(name)) {
        let output = Command::new(env::current_exe().expect("test executable"))
            .args([name, "--exact"])
            .env("O3_TEST_CHILD", name)
            .output()
            .expect("isolated send");
        assert!(output.status.success(), "send child: {output:?}");
        return;
    }
    // SAFETY: this isolated child executes only this test.
    assert_ne!(
        unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) },
        libc::SIG_ERR
    );
    let listener = net::TcpListener::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("listener");
    Kqueue::set_option::<Errno>(listener.as_fd(), libc::SOL_SOCKET, libc::SO_NOSIGPIPE, &0)
        .expect("unprepared listener");
    let _peer =
        net::TcpStream::connect(listener.local_addr().expect("address")).expect("prequeued peer");
    let listener = Kqueue::adopt(listener.into()).expect("adopt listener");
    // SAFETY: both optional address outputs are null.
    let accepted = unsafe { listener.accept(ptr::null_mut(), ptr::null_mut()) }
        .expect("accept prequeued peer");
    let mut suppressed: libc::c_int = 1;
    let mut len = mem::size_of_val(&suppressed) as libc::socklen_t;
    // SAFETY: the owned descriptor and complete scalar output outlive this query.
    assert_eq!(
        unsafe {
            libc::getsockopt(
                accepted.as_fd().as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                ptr::from_mut(&mut suppressed).cast(),
                &mut len,
            )
        },
        0,
    );
    assert_eq!(suppressed, 0);
    Kqueue::shutdown::<Errno>(accepted.as_fd(), socket::How::WRITE).expect("close send half");
    let input = [msg::Iovec::from_slice(b"x")];
    let mut message = msg::Header::new();
    message.bind(&input);
    type Data = <Kqueue as retry::Source>::Send<Context>;
    for data in [
        Data::bytes(bytes::Prefix::new(b"x")),
        Data::message(&message),
    ] {
        // SAFETY: the header and immutable input remain live through both synchronous attempts.
        assert!(matches!(
            unsafe { data.attempt(accepted.as_fd()) },
            retry::Attempt::Ready(Err(error)) if error.code().get() == libc::EPIPE
        ));
    }
}

#[test]
fn cancel_keeps_indices() {
    let Blocked {
        mut files,
        mut state,
        mut poll,
        mut queue,
        mut peer,
        raw,
        buffered,
    } = Blocked::try_new().expect("blocked write fixture");
    let target = state
        .first()
        .expect("blocked write must retain target and fd");
    {
        let owner = state.occupied_fd(raw).expect("original descriptor indexes");
        assert_eq!(Pending::file(owner.record()).fd(), raw);
    }
    assert!(state.occupied_fd(peer.as_raw_fd()).is_none());
    assert!(state.first() == Some(target));
    {
        let entries = state.files().map(|id| (id, change::Filter::Write));
        let outside = File::new(Physical::at::<3>(), peer.as_raw_fd());
        let result = files.try_delete(entries.chain(iter::once((outside, change::Filter::Read))));
        assert!(matches!(
            &result,
            Err(error)
            if error.into_parts() == (Physical::at::<3>().index(), change::Filter::Read)
        ));
    }
    assert!(state.first() == Some(target));
    assert_eq!(files.changes().len(), 1);
    assert!(poll.check().is_ok());
    {
        let prepared = Retry::new(&mut files, &mut state, &mut poll)
            .prepare(NonZeroUsize::MIN)
            .expect("healthy preparation");
        assert!(prepared.is_some(), "pending write");
    }
    assert!(state.first() == Some(target));
    assert!(queue.is_empty());
    let prepared = Retry::new(&mut files, &mut state, &mut poll)
        .prepare(NonZeroUsize::MIN)
        .expect("repeated preparation")
        .expect("original pending write");
    assert_eq!(
        prepared.commit(queue.vacant().expect("terminal vacancy")),
        admit::Submitted
    );
    assert!(state.first().is_none());
    assert!(
        state
            .occupied(target)
            .map(retry::Occupied::remove)
            .is_none()
    );
    {
        let event = queue.pop_front().expect("canceled write");
        let event = match event {
            complete::Dequeued::Emit(event) => Ok(event),
            complete::Dequeued::Reclaim(event) => Err(event),
        }
        .expect("write completion must be emitted");
        assert_eq!(event.key, NonZeroUsize::MIN);
        let result = match event.result {
            Outcome::Sent(result) => Ok(result),
            Outcome::Connected(result) => Err(result),
        }
        .expect("cancelled write must report a send completion");
        assert_eq!(result, Err(Errno::CANCELED));
    }
    assert!(queue.is_empty());
    let mut received = 0;
    loop {
        let mut bytes = [0; 4096];
        match peer.read(&mut bytes) {
            Ok(count) => {
                assert_ne!(count, 0);
                assert!(bytes[..count].iter().all(|&byte| byte == 42));
                received += count;
            }
            Err(error) => {
                assert!(error.is_block(), "drain peer: {error:?}");
                break;
            }
        }
    }
    assert_eq!(received, buffered.get());
}

#[test]
fn write_error_completes() {
    let mut blocked = Blocked::try_new().expect("blocked write fixture");
    // SAFETY: the blocked write retains immutable static payload storage;
    // dispatch consumes its matching key and exact completion vacancy.
    unsafe {
        Retry::new(&mut blocked.files, &mut blocked.state, &mut blocked.poll).dispatch(
            NonZeroUsize::MIN,
            Some(Errno::BAD_DESCRIPTOR),
            blocked.queue.vacant().expect("terminal vacancy"),
        );
    }
    assert!(blocked.state.first().is_none());
    assert!(matches!(
        blocked.queue.pop_front(),
        Some(complete::Dequeued::Emit(Write {
            key,
            result: Outcome::Sent(Err(Errno::BAD_DESCRIPTOR)),
        })) if key == NonZeroUsize::MIN
    ));
    assert_eq!(blocked.poll.check(), Ok(()));
}
