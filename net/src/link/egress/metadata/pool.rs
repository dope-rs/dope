use std::cell::Cell;
use std::mem::{replace, size_of};

use o3::cell::{RegionCell, RegionToken};
use o3::mem::{FairCreditLane, FairCreditPool, FairCreditState};

use super::super::config::Config;

const NONE: u32 = u32::MAX;

struct Value<T>(Option<T>);

impl<T> Unpin for Value<T> {}

struct Metadata {
    next: u32,
    bytes: usize,
    resident: usize,
}

struct Node<'d, T> {
    value: RegionCell<'d, Value<T>>,
    metadata: RegionCell<'d, Metadata>,
}

const _: () = assert!(size_of::<RegionCell<'static, Value<()>>>() == size_of::<Value<()>>());
const _: () = assert!(size_of::<RegionCell<'static, Metadata>>() == size_of::<Metadata>());

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::link::egress) struct ReservedIndex(u32);

impl ReservedIndex {
    pub(in crate::link::egress) const NONE: Self = Self(NONE);

    pub(in crate::link::egress) fn is_none(self) -> bool {
        self == Self::NONE
    }

    pub(in crate::link::egress::metadata) fn into_linked(self) -> LinkedIndex {
        debug_assert!(!self.is_none());
        LinkedIndex(self.0)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::link::egress) struct LinkedIndex(u32);

impl LinkedIndex {
    pub(in crate::link::egress) const NONE: Self = Self(NONE);

    pub(in crate::link::egress) fn is_none(self) -> bool {
        self == Self::NONE
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(in crate::link::egress::metadata) struct DetachedIndex(u32);

pub(in crate::link::egress) struct Pool<'d, T> {
    nodes: Box<[Node<'d, T>]>,
    free: Cell<u32>,
    credits: FairCreditPool<2>,
}

impl<'d, T> Pool<'d, T> {
    pub(in crate::link::egress) fn with_config(token: &RegionToken<'d>, config: Config) -> Self {
        let capacity = config.entries();
        let _ = token;
        Self {
            nodes: (0..capacity)
                .map(|index| Node {
                    value: RegionCell::new(Value(None)),
                    metadata: RegionCell::new(Metadata {
                        next: if index + 1 == capacity {
                            NONE
                        } else {
                            (index + 1) as u32
                        },
                        bytes: 0,
                        resident: 0,
                    }),
                })
                .collect(),
            free: Cell::new(if capacity == 0 { NONE } else { 0 }),
            credits: FairCreditPool::new([
                config.shared_entries as usize,
                config.shared_bytes as usize,
            ]),
        }
    }

    pub(in crate::link::egress::metadata) fn credit<'a>(
        &'a self,
        state: &'a FairCreditState<2>,
    ) -> FairCreditLane<'a, 2> {
        self.credits.bind(state)
    }

    pub(in crate::link::egress) fn reserve(
        &self,
        token: &mut RegionToken<'d>,
        entry: T,
        bytes: usize,
        resident: usize,
    ) -> Result<ReservedIndex, T> {
        let raw = self.free.get();
        if raw == NONE {
            return Err(entry);
        }
        let next = self.nodes[raw as usize].metadata.borrow(token).next;
        self.free.set(next);
        {
            let metadata = self.nodes[raw as usize].metadata.borrow_mut(token);
            metadata.next = NONE;
            metadata.bytes = bytes;
            metadata.resident = resident;
        }
        let value = self.nodes[raw as usize].value.borrow_mut(token);
        debug_assert!(value.0.is_none());
        value.0 = Some(entry);
        Ok(ReservedIndex(raw))
    }

    pub(in crate::link::egress) fn with_value<R>(
        &self,
        token: &RegionToken<'d>,
        index: LinkedIndex,
        inspect: impl FnOnce(&T) -> R,
    ) -> Option<R> {
        self.nodes[index.0 as usize]
            .value
            .borrow(token)
            .0
            .as_ref()
            .map(inspect)
    }

    pub(in crate::link::egress) fn set_reserved_next(
        &self,
        token: &mut RegionToken<'d>,
        index: ReservedIndex,
        next: ReservedIndex,
    ) {
        self.set_next(token, index.0, next.0);
    }

    pub(in crate::link::egress) fn set_linked_next(
        &self,
        token: &mut RegionToken<'d>,
        index: LinkedIndex,
        next: LinkedIndex,
    ) {
        self.set_next(token, index.0, next.0);
    }

    fn set_next(&self, token: &mut RegionToken<'d>, index: u32, next: u32) {
        self.nodes[index as usize].metadata.borrow_mut(token).next = next;
    }

    pub(in crate::link::egress) fn next(
        &self,
        token: &RegionToken<'d>,
        index: LinkedIndex,
    ) -> LinkedIndex {
        LinkedIndex(self.nodes[index.0 as usize].metadata.borrow(token).next)
    }

    pub(in crate::link::egress) fn take_reserved(
        &self,
        token: &mut RegionToken<'d>,
        index: ReservedIndex,
    ) -> Option<(ReservedIndex, T, usize, usize)> {
        self.take_node(token, index.0)
            .map(|(next, value, bytes, resident)| (ReservedIndex(next), value, bytes, resident))
    }

    fn take_node(&self, token: &mut RegionToken<'d>, index: u32) -> Option<(u32, T, usize, usize)> {
        let value = self.nodes[index as usize]
            .value
            .borrow_mut(token)
            .0
            .take()?;
        let metadata = self.nodes[index as usize].metadata.borrow_mut(token);
        let next = metadata.next;
        let bytes = metadata.bytes;
        let resident = metadata.resident;
        metadata.bytes = 0;
        metadata.resident = 0;
        metadata.next = self.free.replace(index);
        Some((next, value, bytes, resident))
    }

    pub(in crate::link::egress::metadata) fn detach_value(
        &self,
        token: &mut RegionToken<'d>,
        index: LinkedIndex,
    ) -> Option<(LinkedIndex, T, usize, usize, DetachedIndex)> {
        let value = self.nodes[index.0 as usize]
            .value
            .borrow_mut(token)
            .0
            .take()?;
        let metadata = self.nodes[index.0 as usize].metadata.borrow_mut(token);
        let next = LinkedIndex(metadata.next);
        let bytes = metadata.bytes;
        let resident = metadata.resident;
        metadata.next = NONE;
        Some((next, value, bytes, resident, DetachedIndex(index.0)))
    }

    pub(in crate::link::egress::metadata) fn restore_value(
        &self,
        token: &mut RegionToken<'d>,
        index: DetachedIndex,
        value: T,
        next: LinkedIndex,
    ) -> LinkedIndex {
        let stored = self.nodes[index.0 as usize].value.borrow_mut(token);
        debug_assert!(stored.0.is_none());
        stored.0 = Some(value);
        self.nodes[index.0 as usize].metadata.borrow_mut(token).next = next.0;
        LinkedIndex(index.0)
    }

    pub(in crate::link::egress::metadata) fn release_detached(
        &self,
        token: &mut RegionToken<'d>,
        index: DetachedIndex,
    ) {
        self.release_empty_node(token, index.0);
    }

    fn release_empty_node(&self, token: &mut RegionToken<'d>, index: u32) {
        debug_assert!(self.nodes[index as usize].value.borrow(token).0.is_none());
        let metadata = self.nodes[index as usize].metadata.borrow_mut(token);
        metadata.bytes = 0;
        metadata.resident = 0;
        metadata.next = self.free.replace(index);
    }

    pub(in crate::link::egress) fn drain_reserved(
        &self,
        token: &mut RegionToken<'d>,
        head: &mut ReservedIndex,
    ) {
        self.drain_nodes(token, replace(head, ReservedIndex::NONE).0);
    }

    pub(in crate::link::egress::metadata) fn drain_linked(
        &self,
        token: &mut RegionToken<'d>,
        head: &mut LinkedIndex,
    ) {
        self.drain_nodes(token, replace(head, LinkedIndex::NONE).0);
    }

    fn drain_nodes(&self, token: &mut RegionToken<'d>, head: u32) {
        let mut drain = NodeDrain {
            pool: self,
            token,
            head,
        };
        while let Some(value) = drain.take() {
            drop(value);
        }
    }

    pub(in crate::link::egress) fn front_consume(
        &self,
        token: &mut RegionToken<'d>,
        index: LinkedIndex,
        bytes: usize,
    ) {
        let metadata = self.nodes[index.0 as usize].metadata.borrow_mut(token);
        debug_assert!(metadata.bytes >= bytes);
        metadata.bytes -= bytes;
    }
}

struct NodeDrain<'a, 'd, T> {
    pool: &'a Pool<'d, T>,
    token: &'a mut RegionToken<'d>,
    head: u32,
}

impl<T> NodeDrain<'_, '_, T> {
    fn take(&mut self) -> Option<T> {
        if self.head == NONE {
            return None;
        }
        let (next, value, _, _) = self.pool.take_node(self.token, self.head)?;
        self.head = next;
        Some(value)
    }
}

impl<T> Drop for NodeDrain<'_, '_, T> {
    fn drop(&mut self) {
        while let Some(value) = self.take() {
            drop(value);
        }
    }
}
