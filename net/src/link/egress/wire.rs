use std::mem::size_of;

use dope_core::io::socket::msg::IoVec;
use o3::cell::RegionToken;

use super::entry::Entry;
use super::metadata;
use super::stage::Stage;
use super::{WireLease, WirePool};

/// A queue-local logical byte range.
///
/// It survives lease compaction.
#[derive(Clone, Copy)]
pub(super) struct Span {
    start: u32,
    len: u32,
}

impl Span {
    pub(super) fn at(base: u32, start: usize, len: usize) -> Self {
        debug_assert!(u32::try_from(start).is_ok());
        debug_assert!(u32::try_from(len).is_ok());
        Self {
            start: base.wrapping_add(start as u32),
            len: len as u32,
        }
    }

    pub(super) fn len(self) -> usize {
        self.len as usize
    }
}

const _: () = assert!(size_of::<Span>() <= size_of::<(*const u8, usize)>());

pub(super) struct Arena<'pool> {
    pool: &'pool WirePool,
}

impl<'pool> Arena<'pool> {
    pub(super) fn new(pool: &'pool WirePool) -> Self {
        Self { pool }
    }

    pub(super) fn state<'a>(
        &'a self,
        lease: &'a mut Option<WireLease<'pool>>,
        base: &'a mut u32,
    ) -> State<'a, 'pool> {
        State::new(self.pool, lease, base)
    }
}

pub(super) struct State<'a, 'pool> {
    pool: &'pool WirePool,
    lease: &'a mut Option<WireLease<'pool>>,
    base: &'a mut u32,
}

impl<'a, 'pool> State<'a, 'pool> {
    fn new(
        pool: &'pool WirePool,
        lease: &'a mut Option<WireLease<'pool>>,
        base: &'a mut u32,
    ) -> Self {
        Self { pool, lease, base }
    }

    pub(super) fn stage<'stage, 'd, B>(
        &'stage mut self,
        entries: metadata::Queue<'stage, 'd, Entry<B>>,
        token: &'stage mut RegionToken<'d>,
    ) -> Stage<'stage, 'd, 'pool, B> {
        self.acquire();
        Stage::open(self.lease, self.base, entries, token)
    }

    pub(super) fn iov(&self, span: Span, offset: usize, cap: usize) -> Option<(IoVec, usize)> {
        let lease = self.lease.as_ref()?;
        let base = *self.base;
        unsafe {
            let start = span.start.wrapping_sub(base) as usize;
            let end = start + span.len();
            debug_assert!(end <= lease.len());
            // SAFETY: The active flight seals this private live span.
            let bytes = lease.as_ref().get_unchecked(start..end);
            Entry::<()>::iov(bytes, offset, cap)
        }
    }

    pub(super) fn try_consume(&mut self, span: Span) -> bool {
        if span.start != *self.base {
            return false;
        }
        let Some(lease) = self.lease.as_mut() else {
            return false;
        };
        let Ok(prefix) = lease.try_consume_prefix(span.len()) else {
            return false;
        };
        prefix.commit();
        *self.base = self.base.wrapping_add(span.len);
        if lease.is_empty() {
            self.lease.take();
            *self.base = 0;
        }
        true
    }

    pub(super) fn reborrow(&mut self) -> State<'_, 'pool> {
        State {
            pool: self.pool,
            lease: self.lease,
            base: self.base,
        }
    }

    fn acquire(&mut self) {
        if self.lease.is_none() {
            *self.lease = self.pool.try_acquire();
            *self.base = 0;
        }
    }
}
