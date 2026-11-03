use std::os::fd::AsFd as _;

use dope_core::{
    Backend, Errno,
    dope_sys::io::{poll, socket, socket::msg},
    io::transfer,
};
use o3::depot::array;

use super::Stream;
use crate::harness::peer::Error;

impl Stream<'_> {
    /// Sends up to eight slices together, capped at the native completion limit.
    pub fn write_vectored(&self, input: &[&[u8]]) -> Result<usize, Error> {
        const { assert!(8 <= <Backend as socket::Socket>::MAX_IOVECS) };
        let count = array::Len::<8>::try_from(input.len()).map_err(Error::Vectors)?;
        let mut vectors = [msg::Iovec::empty(); 8];
        let mut remaining = transfer::MAX_BYTES;
        for (vector, bytes) in count.prefix_mut(&mut vectors).iter_mut().zip(input) {
            let len = bytes.len().min(remaining);
            *vector = msg::Iovec::from_slice(&bytes[..len]);
            remaining -= len;
        }
        if remaining == transfer::MAX_BYTES {
            return Ok(0);
        }
        let mut header = msg::Header::new();
        header.bind(count.prefix(&vectors));
        self.io(
            poll::Entry::writable(self.socket.as_fd()),
            self.write.get(),
            || {
                // SAFETY: the admitted count fits the backend; the exact byte
                // sum is at most MAX_BYTES. Header, vectors and borrowed input
                // remain fixed and live through every synchronous attempt.
                unsafe {
                    <Backend as socket::raw::Source>::sendmsg::<Errno>(
                        self.socket.as_fd(),
                        &header,
                        libc::MSG_NOSIGNAL,
                    )
                }
                .map(|count| count.get() as usize)
            },
        )
    }
}
