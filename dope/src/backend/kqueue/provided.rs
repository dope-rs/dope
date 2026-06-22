pub(super) struct Pool {
    storage: Box<[u8]>,
    buf_len: u32,
    free: Vec<u16>,
}

impl Pool {
    pub(super) fn new(entries: u16, buf_len: u32) -> Self {
        let total = (entries as usize) * (buf_len as usize);
        let storage = vec![0u8; total].into_boxed_slice();
        let free = (0..entries).rev().collect();
        Self {
            storage,
            buf_len,
            free,
        }
    }

    pub(super) fn pop_free(&mut self) -> Option<u16> {
        self.free.pop()
    }

    pub(super) fn ptr_len(&self, bid: u16) -> (*mut u8, usize) {
        let off = (bid as usize) * (self.buf_len as usize);
        // SAFETY: `bid < entries` (caller's invariant); `off < storage.len()`.
        let ptr = unsafe { self.storage.as_ptr().add(off) as *mut u8 };
        (ptr, self.buf_len as usize)
    }

    pub(super) fn defer(&mut self, bid: u16) {
        self.free.push(bid);
    }
}
