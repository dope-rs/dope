use std::{
    cell, io,
    os::{
        fd::{IntoRawFd as _, OwnedFd},
        unix::net,
    },
    time,
};

use dope_sys::{Errno, io::fd};
use io::{Read as _, Write as _};

#[test]
fn transfers_live_fd() {
    let (stream, mut peer) = net::UnixStream::pair().expect("socket pair creation must succeed");
    peer.set_read_timeout(Some(time::Duration::from_secs(5)))
        .expect("read deadline must be available");
    let calls = cell::Cell::new(0);
    let mut byte = [0];
    {
        let owned = unsafe {
            <OwnedFd as fd::raw::Adopt>::try_adopt::<Errno>(stream.into_raw_fd(), || {
                calls.set(calls.get() + 1);
                Errno::BAD_DESCRIPTOR
            })
        }
        .expect("the fresh descriptor must transfer its ownership");
        let mut stream = net::UnixStream::from(owned);
        peer.write_all(b"x")
            .expect("the peer must remain connected");
        stream
            .read_exact(&mut byte)
            .expect("the transferred descriptor must remain live");
        assert_eq!(byte, *b"x");
        assert_eq!(calls.get(), 0);
    }
    assert_eq!(
        peer.read(&mut byte)
            .expect("closing the owner must close its endpoint"),
        0
    );
}

#[test]
fn failure_keeps_errno() {
    let result =
        unsafe { <OwnedFd as fd::raw::Adopt>::try_adopt::<Errno>(-1, || Errno::BAD_DESCRIPTOR) };
    assert!(matches!(result, Err(Errno::BAD_DESCRIPTOR)));
}
