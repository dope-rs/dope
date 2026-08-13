pub(in crate::link) struct Discard {
    remaining: usize,
}

impl Discard {
    pub(super) fn new() -> Self {
        Self { remaining: 0 }
    }

    pub(in crate::link) fn begin(&mut self, bytes: usize) {
        self.remaining = bytes;
    }

    pub(in crate::link) fn remaining(&self) -> usize {
        self.remaining
    }

    pub(in crate::link) fn consume(&mut self, len: usize) -> usize {
        if self.remaining == 0 {
            return 0;
        }
        let take = len.min(self.remaining);
        self.remaining -= take;
        take
    }
}
