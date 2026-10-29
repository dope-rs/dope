use std::{num, os::fd};

use dope_sys::{
    Errno,
    io::{
        reactor::retry::{self, Attempt},
        socket,
        socket::{Socket as _, msg::Header},
    },
    kernel,
};
use o3::buffer::bytes::{self, bounded::MAX};

use crate::{
    Kqueue,
    output::{Data, data::Payload},
};

impl<C: retry::Context> retry::raw::Send<C> for Data<C> {
    fn bytes(input: bytes::Prefix<'_, { MAX }>) -> Self {
        let len = input.len::<{ MAX as u32 }>().into();
        Self(Payload::Bytes {
            ptr: input.into_slice().as_ptr(),
            len,
        })
    }

    fn message(header: &Header) -> Self {
        Self(Payload::Message { msg: header })
    }

    unsafe fn attempt(&self, file: fd::BorrowedFd<'_>) -> Attempt<C::Outcome> {
        let result = match &self.0 {
            Payload::Bytes { ptr, len } => unsafe {
                <Kqueue as socket::raw::Source>::send::<C::Extent, Errno>(file, *ptr, *len)
            },
            Payload::Message { msg } => {
                // SAFETY: the retained pointer stays live through this attempt.
                let header = unsafe { &**msg };
                unsafe { <Kqueue as socket::raw::Source>::sendmsg(file, header, 0) }.map(Into::into)
            }
        };
        match result {
            Ok(count) => Attempt::Ready(C::sent(Ok(count))),
            Err(errno) if errno.is_block() => Attempt::Blocked(errno),
            Err(errno) => Attempt::Ready(C::sent(Err(errno))),
        }
    }

    fn connected(file: fd::BorrowedFd<'_>) -> Result<Attempt<C::Unit>, Errno> {
        use libc::{EALREADY, EINPROGRESS, SO_ERROR};

        let error = Kqueue::get_option::<SO_ERROR, Errno>(file)?;
        let Some(code) = num::NonZero::<i32>::new(error) else {
            return Ok(Attempt::Ready(C::connected(Ok(()))));
        };
        // SAFETY: SO_ERROR returned this unchanged nonzero, positive native error.
        let errno = unsafe { kernel::raw::Code::new(code.get()) }.into();
        if code.get() == EINPROGRESS || code.get() == EALREADY {
            return Ok(Attempt::Blocked(errno));
        }
        Ok(Attempt::Ready(C::connected(Err(errno))))
    }
}
