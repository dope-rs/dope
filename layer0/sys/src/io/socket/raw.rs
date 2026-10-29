use std::{
    os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd},
    slice,
};

use libc::socklen_t;
use o3::{base::num::bounded, buffer, buffer::pool};

use crate::{
    Errno,
    io::{
        fd,
        socket::{Create, Domain, Kind, Setup, accept, msg},
    },
    kernel::{self, errno},
    memory,
};

/// Receives streams or datagrams into caller-owned native storage.
pub trait Receive {
    fn recv<'a>(
        &self,
        output: buffer::raw::Output<'a>,
    ) -> Result<Option<pool::Complete<'a>>, Errno>;

    /// # Safety
    /// Output belongs to a datagram pool with at least sixteen metadata bytes
    /// followed by sockaddr_storage and the admitted payload capacity.
    unsafe fn receive<'a>(
        &self,
        output: buffer::raw::Output<'a>,
    ) -> Result<pool::Complete<'a>, Errno>;
}

pub trait Open: AsFd + Sized {
    fn open(domain: Domain, kind: Kind) -> Result<Self, Create>;

    fn adopt(file: OwnedFd) -> Result<Self, Setup>;

    /// # Safety
    /// Both pointers are null, or name writable full address storage and its returned length.
    /// The caller retains that storage through the returned completion.
    unsafe fn accept(
        &self,
        addr: *mut libc::sockaddr_storage,
        len: *mut socklen_t,
    ) -> Result<Self, accept::Error>;
}

/// Native access to operations retaining caller-owned socket storage.
pub trait Source: errno::Source {
    /// # Safety
    /// Both pointers are null, or name writable sockaddr storage and its capacity
    /// for this call. The caller retains that storage through the returned completion.
    unsafe fn accept<E: From<Errno>>(
        file: BorrowedFd<'_>,
        addr: *mut libc::sockaddr,
        len: *mut socklen_t,
    ) -> Result<OwnedFd, E> {
        let raw = unsafe { libc::accept(file.as_raw_fd(), addr, len) };
        unsafe {
            <OwnedFd as fd::raw::Adopt>::try_adopt(raw, || kernel::raw::Code::last::<Self>().into())
        }
    }

    /// # Safety
    /// The pointer retains a readable input prefix of exactly `len` bytes for this
    /// call. Its original extent preserves the ABI bound and owns the input graph.
    unsafe fn send<X: memory::raw::Extent, E: From<Errno>>(
        file: BorrowedFd<'_>,
        ptr: *const u8,
        len: X::Length,
    ) -> Result<X::Length, E> {
        let len: bounded::U32<0, { i32::MAX as u32 }> = len.into();
        let capacity = len.get() as usize;
        let raw = unsafe { libc::send(file.as_raw_fd(), ptr.cast(), capacity, 0) };
        // SAFETY: send completes at most the admitted c_int-bounded input extent.
        Ok(unsafe {
            kernel::raw::Status::from_count(raw)
                .try_count::<E>(|| kernel::raw::Code::last::<Self>().into())
        }?
        .bounded()
        .into())
    }

    /// # Safety
    /// Header is the frozen root of a live input graph, with a total byte count
    /// bounded by `i32::MAX`; its pointees outlive this call.
    unsafe fn sendmsg<E: From<Errno>>(
        file: BorrowedFd<'_>,
        header: &msg::Header,
        flags: libc::c_int,
    ) -> Result<bounded::U32<0, { i32::MAX as u32 }>, E> {
        let raw = unsafe { libc::sendmsg(file.as_raw_fd(), header.as_raw(), flags) };
        // SAFETY: the retained header has a total extent bounded by c_int.
        Ok(unsafe {
            kernel::raw::Status::from_count(raw)
                .try_count::<E>(|| kernel::raw::Code::last::<Self>().into())
        }?
        .bounded())
    }
}

impl<T: errno::Source> Source for T {}

/// Projection of a native iovec whose retained input graph proves its lifetime.
pub trait Iovec {
    /// # Safety
    /// The descriptor must point to initialized bytes retained for the returned lifetime.
    unsafe fn as_slice<'a>(&self) -> &'a [u8];
}

impl Iovec for msg::Iovec {
    unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        if self.is_empty() {
            return &[];
        }
        // SAFETY: the caller retains this nonempty descriptor's complete input bytes.
        unsafe { slice::from_raw_parts(self.0.iov_base.cast(), self.0.iov_len) }
    }
}
