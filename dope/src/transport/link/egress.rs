use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::slice;

use o3::buffer::Shared;

use crate::backend;
use crate::transport::wire::Vectored;

const EGRESS_CAP_BYTES: usize = 1 << 20;
const EGRESS_CAP_ENTRIES: usize = 4096;

const WIRE_BUF_CAP: usize = 64 * 1024;

struct WireBuf {
    bytes: Box<[MaybeUninit<u8>; WIRE_BUF_CAP]>,
    head: u32,
    tail: u32,
}

impl WireBuf {
    fn new() -> Self {
        Self {
            bytes: Box::new([MaybeUninit::uninit(); WIRE_BUF_CAP]),
            head: 0,
            tail: 0,
        }
    }

    fn len(&self) -> usize {
        (self.tail - self.head) as usize
    }

    fn filled(&self) -> &[u8] {
        let h = self.head as usize;
        let t = self.tail as usize;
        // SAFETY: bytes in [head, tail) were written by `append` before this read.
        unsafe { slice::from_raw_parts(self.bytes.as_ptr().add(h) as *const u8, t - h) }
    }

    fn consume(&mut self, n: usize) {
        self.head += n as u32;
        if self.head == self.tail {
            self.head = 0;
            self.tail = 0;
        }
    }

    fn spare_slice(&mut self) -> &mut [u8] {
        let t = self.tail as usize;
        // SAFETY: [tail, CAP) is owned spare storage; the cursor writes before any read.
        unsafe {
            slice::from_raw_parts_mut(self.bytes.as_mut_ptr().add(t) as *mut u8, WIRE_BUF_CAP - t)
        }
    }
}

pub struct Stage<'a> {
    region: &'a mut [u8],
    len: usize,
    overflowed: bool,
}

impl Stage<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    pub fn push(&mut self, byte: u8) {
        if self.len >= self.region.len() {
            self.overflowed = true;
            return;
        }
        self.region[self.len] = byte;
        self.len += 1;
    }

    pub fn extend_from_slice(&mut self, src: &[u8]) {
        let end = self.len + src.len();
        if end > self.region.len() {
            self.overflowed = true;
            return;
        }
        self.region[self.len..end].copy_from_slice(src);
        self.len = end;
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.region[..self.len]
    }
}

pub struct Queue<const IOV: usize> {
    queue: VecDeque<Shared>,
    wire: Option<WireBuf>,
    partial_sent: u32,
    total_bytes: usize,
    iov_buf: [backend::socket::IoVec; IOV],
    iov_storage: [backend::socket::IoVec; IOV],
    msghdr_storage: backend::socket::MsgHdr,
}

impl<const IOV: usize> Queue<IOV> {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            wire: None,
            partial_sent: 0,
            total_bytes: 0,
            iov_buf: [backend::socket::IoVec::empty(); IOV],
            iov_storage: [backend::socket::IoVec::empty(); IOV],
            msghdr_storage: backend::socket::MsgHdr::empty(),
        }
    }

    pub fn prepare_send(&mut self, bytes_cap: usize) -> Vectored<'_> {
        let n = self.fill_iovs(bytes_cap).len();
        let Self {
            iov_buf,
            iov_storage,
            msghdr_storage,
            ..
        } = self;
        Vectored {
            iovs: &iov_buf[..n],
            iov_storage,
            msghdr_storage,
        }
    }

    pub fn push(&mut self, bytes: Shared) {
        self.total_bytes += bytes.len();
        self.queue.push_back(bytes);
    }

    pub fn wire_stage(&mut self) -> Stage<'_> {
        let wire = self.wire.get_or_insert_with(WireBuf::new);
        Stage {
            region: wire.spare_slice(),
            len: 0,
            overflowed: false,
        }
    }

    pub fn wire_commit(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        let wire = self.wire.as_mut().expect("wire_commit without wire_stage");
        wire.tail += n as u32;
        self.total_bytes += n;
    }

    pub fn over_cap(&self) -> bool {
        self.total_bytes > EGRESS_CAP_BYTES || self.queue.len() > EGRESS_CAP_ENTRIES
    }

    pub fn has_room(&self, entries: usize, bytes: usize) -> bool {
        self.queue.len() + entries <= EGRESS_CAP_ENTRIES
            && self.total_bytes + bytes <= EGRESS_CAP_BYTES
    }

    pub fn fill_iovs(&mut self, bytes_cap: usize) -> &[backend::socket::IoVec] {
        let cap = bytes_cap.min(u32::MAX as usize);
        let mut n = 0usize;
        let mut bytes = 0usize;
        let queue_len = self.queue.len();
        for (i, b) in self.queue.iter().enumerate() {
            if n == IOV || bytes >= cap {
                break;
            }
            let off = if i == 0 {
                self.partial_sent as usize
            } else {
                0
            };
            let slice = b.as_slice();
            if off >= slice.len() {
                continue;
            }
            let avail = slice.len() - off;
            let take = avail.min(cap - bytes);
            self.iov_buf[n] = backend::socket::IoVec::from_slice(&slice[off..off + take]);
            bytes += take;
            n += 1;
            if take < avail {
                return &self.iov_buf[..n];
            }
        }
        if n < IOV
            && bytes < cap
            && let Some(wire) = self.wire.as_ref()
        {
            let off = if queue_len == 0 {
                self.partial_sent as usize
            } else {
                0
            };
            let slice = wire.filled();
            if off < slice.len() {
                let avail = slice.len() - off;
                let take = avail.min(cap - bytes);
                self.iov_buf[n] = backend::socket::IoVec::from_slice(&slice[off..off + take]);
                n += 1;
            }
        }
        &self.iov_buf[..n]
    }

    pub fn ack(&mut self, n: usize) {
        let mut left = n as u64;
        self.total_bytes = self.total_bytes.saturating_sub(n);
        while left > 0 {
            let Some(head) = self.queue.front() else {
                break;
            };
            let head_len = head.len() as u64;
            let already = self.partial_sent as u64;
            let remaining = head_len - already;
            if left >= remaining {
                left -= remaining;
                self.partial_sent = 0;
                self.queue.pop_front();
            } else {
                self.partial_sent += left as u32;
                left = 0;
            }
        }
        if left > 0
            && let Some(wire) = self.wire.as_mut()
        {
            let unsent = wire.len() as u64;
            let already = self.partial_sent as u64;
            let take = (already + left).min(unsent);
            wire.consume(take as usize);
            self.partial_sent = 0;
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn pending_at(&self, idx: usize) -> Shared {
        let Some(b) = self.queue.get(idx) else {
            return Shared::new();
        };
        let off = if idx == 0 {
            self.partial_sent as usize
        } else {
            0
        };
        if off >= b.len() {
            Shared::new()
        } else {
            b.slice(off..)
        }
    }
}

impl<const IOV: usize> Default for Queue<IOV> {
    fn default() -> Self {
        Self::new()
    }
}
