use std::{
    io::{self, ErrorKind, Read as _, Write as _},
    num::NonZeroUsize,
    os::{fd::AsFd as _, unix::net::UnixStream},
    time,
};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno,
    io::{reactor::ready::Poll as _, socket, socket::Prepare as _},
    table::{self, Files as _, change},
};
use o3::base::num::bounded;

use crate::common::{
    Files, Poll,
    slots::{
        Slots,
        inputs::{Owned, Physical},
    },
};

type Handle = <Files as table::Files<Slots>>::Handle;
type Index = <Slots as change::raw::Slots>::Index;
type Pair = (Option<UnixStream>, UnixStream);

#[derive(Debug)]
pub enum Error {
    Pair(io::Error),
    Timeout(io::Error),
    Nonblock(io::Error),
    Clone(io::Error),
    Write(io::Error),
    Read(io::Error),
    Adopt(socket::Setup),
    Install(table::Install<Index>),
    Arm(Errno),
    Missing(Physical),
    WriteZero,
    Empty,
    Overflow(bounded::Error),
}

pub fn try_pair() -> Result<(Handle, UnixStream), Error> {
    let (stream, peer) = UnixStream::pair().map_err(Error::Pair)?;
    peer.set_read_timeout(Some(time::Duration::from_secs(5)))
        .map_err(Error::Timeout)?;
    let handle = Kqueue::adopt(stream.into()).map_err(Error::Adopt)?;
    Ok((handle, peer))
}

pub fn try_exchange(
    files: &Files,
    slot: Physical,
    peer: &mut UnixStream,
) -> Result<[u8; 1], Error> {
    let owned = files
        .borrow(slot)
        .ok_or(Error::Missing(slot))?
        .try_clone_to_owned()
        .map_err(Error::Clone)?;
    let mut stream = UnixStream::from(owned);
    peer.write_all(b"x").map_err(Error::Write)?;
    let mut byte = [0];
    stream.read_exact(&mut byte).map_err(Error::Read)?;
    Ok(byte)
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
    let (read, write) = UnixStream::pair().map_err(Error::Pair)?;
    let read = if token == NonZeroUsize::MIN {
        let slot = Physical::at::<1>();
        let handle = Kqueue::adopt(read.into()).map_err(Error::Adopt)?;
        files
            .install(Owned::from(slot), handle)
            .map_err(Error::Install)?;
        poll.arm_shutdown(files.borrow(slot).ok_or(Error::Missing(slot))?, token)
            .map_err(Error::Arm)?;
        None
    } else {
        poll.arm_shutdown(read.as_fd(), token).map_err(Error::Arm)?;
        Some(read)
    };
    Ok((read, write))
}

pub fn try_blocked() -> Result<(Handle, UnixStream, NonZeroUsize), Error> {
    let (socket, peer) = try_pair()?;
    peer.set_nonblocking(true).map_err(Error::Nonblock)?;
    let mut writer = UnixStream::from(socket.as_fd().try_clone_to_owned().map_err(Error::Clone)?);
    let mut buffered = bounded::Usize::<{ usize::MAX }>::ZERO;
    loop {
        match writer.write(&[42; 4096]) {
            Ok(0) => return Err(Error::WriteZero),
            Ok(count) => {
                buffered = buffered.try_add(count).map_err(Error::Overflow)?;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => break,
            Err(error) => return Err(Error::Write(error)),
        }
    }
    Ok((
        socket,
        peer,
        NonZeroUsize::new(buffered.get()).ok_or(Error::Empty)?,
    ))
}
