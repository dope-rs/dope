use std::{
    num,
    os::fd::{AsFd as _, AsRawFd as _},
};

use dope_sys::{
    Errno,
    io::{socket, socket::Socket as _},
};
use libc::socklen_t;
use o3::buffer::{self, pool};

use crate::{Kqueue, sockets::Handle};

impl socket::raw::Receive for Handle {
    fn recv<'a>(
        &self,
        mut output: buffer::raw::Output<'a>,
    ) -> Result<Option<pool::Complete<'a>>, Errno> {
        let bytes = Kqueue::recv::<0, Errno>(self.as_fd(), output.as_mut_slice())?;
        let Some(actual) = num::NonZero::new(bytes.len()) else {
            return Ok(None);
        };
        // SAFETY: recv returned this positive prefix of the exact borrowed output.
        Ok(Some(unsafe { output.complete(actual) }))
    }

    unsafe fn receive<'a>(
        &self,
        mut output: buffer::raw::Output<'a>,
    ) -> Result<pool::Complete<'a>, Errno> {
        use std::{mem, ptr};

        use dope_sys::kernel::raw::{Code, Status};

        const NAME: usize = mem::size_of::<libc::sockaddr_storage>();
        const MIN: usize = 16 + NAME;
        // SAFETY: the caller's contract reserves at least sixteen metadata bytes
        // followed by sockaddr_storage, so this split point stays in bounds.
        let (prefix, body) = unsafe { output.as_mut_slice().split_at_mut_unchecked(16) };
        // SAFETY: the same contract places a full sockaddr_storage after those
        // metadata bytes, so the name/payload split also stays in bounds.
        let (name, payload) = unsafe { body.split_at_mut_unchecked(NAME) };
        let mut iov = libc::iovec {
            iov_base: payload.as_mut_ptr().cast(),
            iov_len: payload.len(),
        };
        let mut header = libc::msghdr {
            msg_name: name.as_mut_ptr().cast(),
            msg_namelen: NAME as socklen_t,
            msg_iov: ptr::from_mut(&mut iov),
            msg_iovlen: 1,
            msg_control: ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };
        let raw = unsafe { libc::recvmsg(self.as_fd().as_raw_fd(), &mut header, 0) };
        // SAFETY: the originating memory::raw::Layout bounds this output by
        // i32::MAX. recvmsg completes at most its exact payload.len(), or returns
        // -1 with unchanged errno.
        let received = unsafe {
            Status::from_count(raw).try_count::<Errno>(|| Code::last::<Kqueue>().into())
        }?
        .get();
        let total = num::NonZero::<usize>::MIN.saturating_add(MIN - 1 + received);
        prefix[0..4].copy_from_slice(&(header.msg_namelen as u32).to_ne_bytes());
        prefix[4..8].copy_from_slice(&0u32.to_ne_bytes());
        prefix[8..12].copy_from_slice(&(received as u32).to_ne_bytes());
        prefix[12..16].copy_from_slice(&(header.msg_flags as u32).to_ne_bytes());
        Ok(unsafe { output.complete(total) })
    }
}
