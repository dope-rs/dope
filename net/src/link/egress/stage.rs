use o3::buffer::Shared;

use super::metadata::MetadataQueue;
use super::queue::{Entry, PreparedChain};
use super::wire::WireBuf;

pub struct Stage<'a, B = Shared> {
    wire: &'a mut Option<WireBuf>,
    entries: &'a MetadataQueue<Entry<B>>,
    start: usize,
    len: usize,
    overflowed: bool,
    committed: bool,
}

impl<'a, B> Stage<'a, B> {
    pub(super) fn open(
        wire: &'a mut Option<WireBuf>,
        entries: &'a MetadataQueue<Entry<B>>,
    ) -> Self {
        let (start, overflowed) = match wire.as_mut() {
            Some(buffer) => (buffer.len(), false),
            None => (0, true),
        };
        Self {
            wire,
            entries,
            start,
            len: 0,
            overflowed,
            committed: false,
        }
    }
}

impl<B> Stage<'_, B> {
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
        if self.overflowed {
            return;
        }
        let Some(buffer) = self.wire.as_mut() else {
            self.overflowed = true;
            return;
        };
        if buffer.contiguous_spare_writer().try_push(byte).is_err() {
            self.overflowed = true;
            return;
        }
        self.len += 1;
    }

    pub fn extend_from_slice(&mut self, src: &[u8]) {
        if self.overflowed {
            return;
        }
        let Some(buffer) = self.wire.as_mut() else {
            self.overflowed = true;
            return;
        };
        if buffer
            .contiguous_spare_writer()
            .try_extend_from_slice(src)
            .is_err()
        {
            self.overflowed = true;
            return;
        }
        self.len += src.len();
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let Some(buffer) = self.wire.as_mut() else {
            return &mut [];
        };
        &mut buffer.as_mut_slice()[self.start..self.start + self.len]
    }
}

impl<B: AsRef<[u8]>> Stage<'_, B> {
    pub fn commit(self) -> usize {
        self.commit_with(None)
    }

    pub(super) fn commit_with(mut self, body: Option<B>) -> usize {
        if self.overflowed || self.len == 0 {
            return 0;
        }
        let Some(buffer) = self.wire.as_ref() else {
            return 0;
        };
        let data = unsafe { buffer.as_ptr().add(self.start) };
        let mut prepared = PreparedChain::new(&self.entries.arena.pool);
        if !prepared.push_wire(data, self.len) {
            return 0;
        }
        if let Some(body) = body
            && !prepared.push(body)
        {
            return 0;
        }
        if !prepared.commit(self.entries) {
            return 0;
        }
        self.committed = true;
        self.len
    }
}

impl<B> Drop for Stage<'_, B> {
    fn drop(&mut self) {
        if !self.committed
            && let Some(buffer) = self.wire.as_mut()
        {
            buffer.truncate(self.start);
        }
        if self.wire.as_ref().is_some_and(WireBuf::is_empty) {
            self.wire.take();
        }
    }
}
