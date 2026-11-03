use std::{
    mem,
    num::NonZeroUsize,
    os::fd::{self, AsFd as _, AsRawFd as _},
    ptr,
};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno, Nonblock,
    io::{
        fd::Fd as _, reactor::ready::Poll as _, socket, socket::Prepare as _, socket::Socket as _,
    },
    kernel::raw::{Code, Status},
    table::{self, Files as _, change},
};
use o3::base::{marker::ThreadBound, num::bounded};

use crate::common::{
    Files, Poll,
    slots::{
        Slots,
        inputs::{Owned, Physical},
    },
};

type Handle = <Files as table::Files<Slots>>::Handle;
type Index = <Slots as change::raw::Slots>::Index;
type Pair = (Option<Peer>, Peer);

#[derive(Debug)]
pub enum Error {
    Pair(socket::record::Error),
    Timeout(Errno),
    Nonblock(Nonblock),
    Write(Errno),
    Read(Errno),
    Adopt(socket::Setup),
    Install(table::Install<Index>),
    Arm(Errno),
    Missing(Physical),
    WriteZero,
    UnexpectedEof,
    Empty,
    Overflow(bounded::Error),
}

pub struct Peer {
    file: fd::OwnedFd,
    _thread: ThreadBound,
}

impl Peer {
    fn new(file: fd::OwnedFd) -> Self {
        Self {
            file,
            _thread: ThreadBound::NEW,
        }
    }

    pub fn read(&mut self, output: &mut [u8]) -> Result<usize, Errno> {
        read(self.as_fd(), output)
    }

    pub fn write_all(&mut self, mut input: &[u8]) -> Result<(), Error> {
        while !input.is_empty() {
            match Kqueue::send_quiet::<Errno>(self.as_fd(), input) {
                Ok([]) => return Err(Error::WriteZero),
                Ok(bytes) => input = &input[bytes.len()..],
                Err(error) if error.is_intr() => {}
                Err(error) => return Err(Error::Write(error)),
            }
        }
        Ok(())
    }
}

impl fd::AsFd for Peer {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.file.as_fd()
    }
}

impl fd::AsRawFd for Peer {
    fn as_raw_fd(&self) -> fd::RawFd {
        self.file.as_raw_fd()
    }
}

pub fn try_pair() -> Result<(Handle, Peer), Error> {
    let (stream, peer) = Kqueue::pair().map_err(Error::Pair)?;
    let timeout = libc::timeval {
        tv_sec: 5,
        tv_usec: 0,
    };
    // SAFETY: the live socket and complete timeval stay borrowed through setsockopt;
    // its result is zero or -1, with the failing errno captured immediately.
    unsafe {
        Status::new(libc::setsockopt(
            peer.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            ptr::from_ref(&timeout).cast(),
            mem::size_of::<libc::timeval>() as libc::socklen_t,
        ))
        .try_zero::<Errno>(|| Code::last::<Kqueue>().into())
    }
    .map_err(Error::Timeout)?;
    let handle = Kqueue::adopt(stream).map_err(Error::Adopt)?;
    Ok((handle, Peer::new(peer)))
}

pub fn try_exchange(files: &Files, slot: Physical, peer: &mut Peer) -> Result<[u8; 1], Error> {
    let stream = files.borrow(slot).ok_or(Error::Missing(slot))?;
    peer.write_all(b"x")?;
    let mut byte = [0];
    match read(stream, &mut byte).map_err(Error::Read)? {
        0 => Err(Error::UnexpectedEof),
        _ => Ok(byte),
    }
}

fn read(file: fd::BorrowedFd<'_>, output: &mut [u8]) -> Result<usize, Errno> {
    loop {
        match Kqueue::read::<Errno>(file, output) {
            Err(error) if error.is_intr() => {}
            result => return result.map(|bytes| bytes.len()),
        }
    }
}

pub fn try_pairs(poll: &mut Poll, files: &mut Files) -> Result<[Pair; 3], Error> {
    let key = NonZeroUsize::MIN;
    Ok([
        try_arm(poll, files, key)?,
        try_arm(poll, files, key.saturating_add(1))?,
        try_arm(poll, files, key.saturating_add(2))?,
    ])
}

fn try_arm(poll: &mut Poll, files: &mut Files, token: NonZeroUsize) -> Result<Pair, Error> {
    let (read, write) = Kqueue::pair().map_err(Error::Pair)?;
    let read = if token == NonZeroUsize::MIN {
        let slot = Physical::at::<1>();
        let handle = Kqueue::adopt(read).map_err(Error::Adopt)?;
        files
            .install(Owned::from(slot), handle)
            .map_err(Error::Install)?;
        poll.arm_shutdown(files.borrow(slot).ok_or(Error::Missing(slot))?, token)
            .map_err(Error::Arm)?;
        None
    } else {
        poll.arm_shutdown(read.as_fd(), token).map_err(Error::Arm)?;
        Some(Peer::new(read))
    };
    Ok((read, Peer::new(write)))
}

pub fn try_blocked() -> Result<(Handle, Peer, NonZeroUsize), Error> {
    let (socket, peer) = try_pair()?;
    Kqueue::update::<Nonblock>(peer.as_fd()).map_err(Error::Nonblock)?;
    let mut buffered = bounded::Usize::<{ usize::MAX }>::ZERO;
    loop {
        match Kqueue::send_quiet::<Errno>(socket.as_fd(), &[42; 4096]) {
            Ok([]) => return Err(Error::WriteZero),
            Ok(bytes) => buffered = buffered.try_add(bytes.len()).map_err(Error::Overflow)?,
            Err(error) if error.is_block() => break,
            Err(error) if error.is_intr() => {}
            Err(error) => return Err(Error::Write(error)),
        }
    }
    Ok((
        socket,
        peer,
        NonZeroUsize::new(buffered.get()).ok_or(Error::Empty)?,
    ))
}
