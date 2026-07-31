use o3::buffer::Shared;

use super::WireLease;
use super::arena::PreparedChain;
use super::metadata::MetadataQueue;
use super::raw::entry::Entry;
use super::raw::wire::WirePointer;

pub struct Stage<'a, 'pool, B = Shared> {
    lease: &'a mut Option<WireLease<'pool>>,
    entries: MetadataQueue<'a, Entry<B>>,
    start: usize,
    len: usize,
    overflowed: bool,
    committed: bool,
}

impl<'a, 'pool, B> Stage<'a, 'pool, B> {
    pub(super) fn open(
        lease: &'a mut Option<WireLease<'pool>>,
        entries: MetadataQueue<'a, Entry<B>>,
    ) -> Self {
        let (start, overflowed) = match lease.as_mut() {
            Some(buffer) => (buffer.len(), false),
            None => (0, true),
        };
        Self {
            lease,
            entries,
            start,
            len: 0,
            overflowed,
            committed: false,
        }
    }
}

impl<B> Stage<'_, '_, B> {
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
        let Some(buffer) = self.lease.as_mut() else {
            self.overflowed = true;
            return;
        };
        if buffer.try_push(byte).is_err() {
            self.overflowed = true;
            return;
        }
        self.len += 1;
    }

    pub fn extend_from_slice(&mut self, src: &[u8]) {
        if self.overflowed {
            return;
        }
        let Some(buffer) = self.lease.as_mut() else {
            self.overflowed = true;
            return;
        };
        if buffer.try_extend_from_slice(src).is_err() {
            self.overflowed = true;
            return;
        }
        self.len += src.len();
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let Some(buffer) = self.lease.as_mut() else {
            return &mut [];
        };
        &mut buffer.as_mut_slice()[self.start..self.start + self.len]
    }
}

impl<B: AsRef<[u8]>> Stage<'_, '_, B> {
    pub fn commit(self) -> usize {
        self.commit_with(None::<B>)
    }

    pub(super) fn commit_with(mut self, body: Option<B>) -> usize {
        if self.overflowed || self.len == 0 {
            return 0;
        }
        let Some(buffer) = self.lease.as_ref() else {
            return 0;
        };
        let data = WirePointer::at(buffer, self.start);
        let mut prepared = PreparedChain::new(self.entries.pool);
        if !prepared.push_wire(data.get(), self.len) {
            return 0;
        }
        if let Some(body) = body
            && !prepared.push(body)
        {
            return 0;
        }
        if !prepared.commit(&self.entries) {
            return 0;
        }
        self.committed = true;
        self.len
    }
}

impl<B> Drop for Stage<'_, '_, B> {
    fn drop(&mut self) {
        if !self.committed
            && let Some(buffer) = self.lease.as_mut()
        {
            buffer.truncate(self.start);
        }
        if self.lease.as_ref().is_some_and(WireLease::is_empty) {
            self.lease.take();
        }
    }
}
