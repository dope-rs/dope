use super::super::queue::Queue;
use crate::wire::send::Vectored;

pub(in crate::link::egress) struct Preparation<'a, const IOV: usize, B> {
    queue: &'a mut Queue<IOV, B>,
    len: usize,
}

impl<'a, const IOV: usize, B: AsRef<[u8]>> Preparation<'a, IOV, B> {
    pub(in crate::link::egress) fn new(queue: &'a mut Queue<IOV, B>, len: usize) -> Self {
        Self { queue, len }
    }

    pub(in crate::link::egress) fn prepare(self) -> Vectored<'a> {
        let (iovs, iov_storage, msghdr_storage) = self.queue.iov_parts(self.len);
        // SAFETY: every descriptor was derived from this queue's live entry
        // storage. The exclusive queue borrow prevents removal for `'a`; the
        // consuming send protocol retains entries until their completion.
        unsafe { Vectored::from_raw(iovs, iov_storage, msghdr_storage) }
    }
}
