use o3::buffer::Shared;
use o3::cell::RegionToken;

use super::StableBytes;
use super::WireLease;
use super::arena::PreparedChain;
use super::entry::Entry;
use super::metadata;
use super::wire::Span;

pub struct Stage<'a, 'd, 'pool, B = Shared> {
    lease: Option<&'a mut Option<WireLease<'pool>>>,
    base: Option<&'a mut u32>,
    entries: metadata::Queue<'a, 'd, Entry<B>>,
    token: &'a mut RegionToken<'d>,
    start: usize,
    len: usize,
    overflowed: bool,
    committed: bool,
}

impl<'a, 'd, 'pool, B> Stage<'a, 'd, 'pool, B> {
    pub(super) fn open(
        lease: &'a mut Option<WireLease<'pool>>,
        base: &'a mut u32,
        entries: metadata::Queue<'a, 'd, Entry<B>>,
        token: &'a mut RegionToken<'d>,
    ) -> Self {
        let (start, overflowed) = match lease.as_mut() {
            Some(buffer) => (buffer.len(), false),
            None => (0, true),
        };
        Self {
            lease: Some(lease),
            base: Some(base),
            entries,
            token,
            start,
            len: 0,
            overflowed,
            committed: false,
        }
    }

    pub(super) fn blocked(
        entries: metadata::Queue<'a, 'd, Entry<B>>,
        token: &'a mut RegionToken<'d>,
    ) -> Self {
        Self {
            lease: None,
            base: None,
            entries,
            token,
            start: 0,
            len: 0,
            overflowed: true,
            committed: false,
        }
    }
}

impl<B> Stage<'_, '_, '_, B> {
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
        let Some(buffer) = self.lease.as_deref_mut().and_then(Option::as_mut) else {
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
        let Some(buffer) = self.lease.as_deref_mut().and_then(Option::as_mut) else {
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
        let Some(buffer) = self.lease.as_deref_mut().and_then(Option::as_mut) else {
            return &mut [];
        };
        &mut buffer.as_mut_slice()[self.start..self.start + self.len]
    }
}

impl<B: StableBytes> Stage<'_, '_, '_, B> {
    pub fn commit(self) -> usize {
        self.commit_with(None::<B>)
    }

    pub(super) fn commit_with(mut self, body: Option<B>) -> usize {
        if self.overflowed || self.len == 0 {
            return 0;
        }
        let Some(buffer) = self.lease.as_deref().and_then(Option::as_ref) else {
            return 0;
        };
        let Some(base) = self.base.as_deref() else {
            return 0;
        };
        debug_assert!(self.start + self.len <= buffer.len());
        let span = Span::at(*base, self.start, self.len);
        let mut prepared = PreparedChain::new(self.entries.pool, self.token);
        if !prepared.push_wire(span) {
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

impl<B> Drop for Stage<'_, '_, '_, B> {
    fn drop(&mut self) {
        if !self.committed
            && let Some(buffer) = self.lease.as_deref_mut().and_then(Option::as_mut)
        {
            buffer.truncate(self.start);
        }
        if self
            .lease
            .as_deref()
            .and_then(Option::as_ref)
            .is_some_and(WireLease::is_empty)
        {
            if let Some(lease) = self.lease.as_deref_mut() {
                lease.take();
            }
        }
    }
}
