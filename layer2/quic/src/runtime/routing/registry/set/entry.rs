use crate::runtime::routing::registry;
use std::time;

use o3::buffer;
use o3::depot::{
    heap::indexed,
    queue::indexed::planes::{bound, resident},
};

use crate::runtime::routing::registry::Key;
use crate::runtime::routing::registry::set::Live;
use crate::runtime::{brand, mux};
use crate::{conn, stream};
use conn::life::status::schedule;

pub(in crate::runtime) struct Entries<
    'storage,
    'id,
    C,
    const DOMAIN: u8,
    B: stream::Recv,
    D: buffer::raw::Segment,
    T,
> {
    pub(super) source: Live<'storage, 'id, C, DOMAIN, B, D, T>,
    pub(super) counts: &'storage mut super::Counts,
}

impl<'storage, 'id, C, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment, T>
    Entries<'storage, 'id, C, DOMAIN, B, D, T>
{
    pub(in crate::runtime) fn admit(&self, key: bound::ActiveKey<'id>) -> Option<Key<'id>> {
        self.source.admit(key).map(Key)
    }

    pub(in crate::runtime) fn access<'loan, const TRACK: bool>(
        &'loan mut self,
        key: Key<'id>,
        brand: brand::Token<'loan>,
    ) -> registry::Access<'loan, C, DOMAIN, B, D, T, TRACK> {
        self.source.get_mut(key.0).access(brand)
    }

    pub(in crate::runtime) fn refresh(
        &mut self,
        key: Key<'id>,
        now: time::Instant,
        update: conn::DeadlineMode,
        retry: Option<time::Instant>,
    ) {
        let (timer, mut entry) = self.source.select(key.0);
        if entry.get().conn.view().lifecycle().is_closing()
            && entry.get().flags.notification != registry::Notification::Retained
            && entry.metadata().next().is_some()
        {
            entry.get_mut().flags.notification = registry::Notification::Retained;
            // This first terminal transition contributes one occupied,
            // reusable slot; the fixed admitted domain bounds the count.
            self.counts.retained += 1;
        }
        entry.unlink::<{ mux::REAP_QUEUE }>();
        let slot = entry.get();
        if slot.conn.view().lifecycle().is_closed() {
            entry.link_back::<{ mux::REAP_QUEUE }>();
            if let indexed::Entry::Occupied(timer) = timer {
                timer.retire();
            }
            return;
        }
        let flush = entry.is_linked::<{ mux::FLUSH_QUEUE }>();
        let due = schedule::deadline_with_retry(slot.conn.machine(), now, !flush, retry);
        match (timer, due, update) {
            (indexed::Entry::Vacant(timer), Some(due), _) => {
                timer.set(due);
            }
            (indexed::Entry::Occupied(timer), Some(due), conn::DeadlineMode::Earlier)
                if due < *timer.priority() =>
            {
                timer.set(due);
            }
            (indexed::Entry::Occupied(timer), Some(due), conn::DeadlineMode::Exact)
                if due != *timer.priority() =>
            {
                timer.set(due);
            }
            (indexed::Entry::Occupied(timer), None, conn::DeadlineMode::Exact) => {
                timer.retire();
            }
            _ => {}
        }
    }
}

pub(in crate::runtime) struct Entry<
    'storage,
    C,
    const DOMAIN: u8,
    B: stream::Recv,
    D: buffer::raw::Segment,
    T,
> {
    pub(super) inner: resident::Entry<
        'storage,
        registry::Slot<C, DOMAIN, B, D, T>,
        registry::Epoch,
        { mux::QUEUE_PLANES },
        false,
        { mux::FREE_QUEUE },
    >,
}

impl<C, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment, T>
    Entry<'_, C, DOMAIN, B, D, T>
{
    pub(in crate::runtime) fn access<'loan, const TRACK: bool>(
        &'loan mut self,
        brand: brand::Token<'loan>,
    ) -> registry::Access<'loan, C, DOMAIN, B, D, T, TRACK> {
        self.inner.get_mut().access(brand)
    }

    pub(in crate::runtime) fn is_linked<const PLANE: usize>(&self) -> bool {
        self.inner.is_linked::<PLANE>()
    }

    pub(in crate::runtime) fn link_back<const PLANE: usize>(&mut self) {
        self.inner.link_back::<PLANE>();
    }

    pub(in crate::runtime) fn unlink<const PLANE: usize>(&mut self) {
        self.inner.unlink::<PLANE>();
    }
}
