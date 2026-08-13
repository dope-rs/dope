use std::{array, cell, mem, process};

use o3::{cell::region, collections};

use crate::link::{egress, egress::metadata::free};

pub(in crate::link::egress) mod indices;

const NONE: u32 = u32::MAX;

struct Value<T>(Option<T>);

impl<T> Unpin for Value<T> {}

struct NodeState {
    next: u32,
    bytes: usize,
    resident: usize,
}

struct Node<'d, T> {
    value: region::Value<'d, Value<T>>,
    metadata: region::Value<'d, NodeState>,
}

struct CreditDomain {
    shared: cell::Cell<[usize; 2]>,
    protected: [usize; 2],
    lanes: usize,
}

pub(in crate::link::egress) struct CreditState {
    reserve: [usize; 2],
    held: cell::Cell<[usize; 2]>,
}

#[derive(Clone, Copy)]
pub(in crate::link::egress) struct CreditLane<'a> {
    shared: &'a cell::Cell<[usize; 2]>,
    state: &'a CreditState,
}

const _: () =
    assert!(mem::size_of::<region::Value<'static, Value<()>>>() == mem::size_of::<Value<()>>());
const _: () =
    assert!(mem::size_of::<region::Value<'static, NodeState>>() == mem::size_of::<NodeState>());

pub(in crate::link::egress) struct Pool<'d, T> {
    nodes: Box<[Node<'d, T>]>,
    free: free::Free,
    credits: CreditDomain,
}

impl CreditDomain {
    fn new(shared: [usize; 2], protected: [usize; 2], lanes: usize) -> Self {
        Self {
            shared: cell::Cell::new(shared),
            protected,
            lanes,
        }
    }

    fn contains(&self, lane: usize) -> bool {
        lane < self.lanes
    }

    fn state(&self, lane: usize) -> CreditState {
        CreditState {
            reserve: array::from_fn(|dimension| {
                self.protected[dimension] / self.lanes
                    + usize::from(lane < self.protected[dimension] % self.lanes)
            }),
            held: cell::Cell::new([0; 2]),
        }
    }

    fn lane<'a>(&'a self, state: &'a CreditState) -> CreditLane<'a> {
        CreditLane {
            shared: &self.shared,
            state,
        }
    }
}

impl CreditLane<'_> {
    pub(in crate::link::egress::metadata) fn try_acquire(self, amount: [usize; 2]) -> bool {
        let held = self.state.held.get();
        let shared = self.shared.get();
        let mut next = [0; 2];
        let mut borrowed = [0; 2];
        for dimension in 0..2 {
            let Some(next_held) = held[dimension].checked_add(amount[dimension]) else {
                return false;
            };
            next[dimension] = next_held;
            let own = self.state.reserve[dimension].saturating_sub(held[dimension]);
            borrowed[dimension] = amount[dimension].saturating_sub(own);
            if borrowed[dimension] > shared[dimension] {
                return false;
            }
        }
        self.state.held.set(next);
        self.shared.set(array::from_fn(|dimension| {
            shared[dimension] - borrowed[dimension]
        }));
        true
    }

    pub(in crate::link::egress::metadata) fn release(self, amount: [usize; 2]) {
        let held = self.state.held.get();
        let mut next = [0; 2];
        for dimension in 0..2 {
            let Some(remaining) = held[dimension].checked_sub(amount[dimension]) else {
                process::abort();
            };
            next[dimension] = remaining;
        }
        let returned: [usize; 2] = array::from_fn(|dimension| {
            amount[dimension].min(held[dimension].saturating_sub(self.state.reserve[dimension]))
        });
        self.state.held.set(next);
        let shared = self.shared.get();
        self.shared.set(array::from_fn(|dimension| {
            shared[dimension] + returned[dimension]
        }));
    }
}

impl<'d, T> Pool<'d, T> {
    pub(in crate::link::egress) fn try_with_config(
        token: &region::Token<'d>,
        config: egress::Config,
        lanes: usize,
    ) -> Result<Self, collections::AllocationError> {
        let entries = config.entry_capacity();
        let capacity = entries as usize;
        let _ = token;
        Ok(Self {
            nodes: collections::BoxSliceExt::try_box_with(capacity, |index| {
                let next = index as u32 + 1;
                Node {
                    value: region::Value::new(Value(None)),
                    metadata: region::Value::new(NodeState {
                        next: if next == entries { NONE } else { next },
                        bytes: 0,
                        resident: 0,
                    }),
                }
            })?,
            free: free::Free::new(if entries == 0 { NONE } else { 0 }),
            credits: CreditDomain::new(
                [config.shared_entries as usize, config.shared_bytes as usize],
                [
                    config.reserved_entries as usize,
                    config.reserved_bytes as usize,
                ],
                lanes,
            ),
        })
    }

    pub(in crate::link::egress) fn contains_lane(&self, lane: usize) -> bool {
        self.credits.contains(lane)
    }

    pub(in crate::link::egress::metadata) fn credit<'a>(
        &'a self,
        state: &'a CreditState,
    ) -> CreditLane<'a> {
        self.credits.lane(state)
    }

    pub(in crate::link::egress) fn credit_state(&self, lane: usize) -> CreditState {
        self.credits.state(lane)
    }

    pub(in crate::link::egress) fn reserve_mapped<U>(
        &self,
        token: &mut region::Token<'d>,
        value: U,
        bytes: usize,
        resident: usize,
        map: impl FnOnce(U) -> T,
    ) -> Result<indices::ReservedIndex<'d>, U> {
        let raw = self.free.get();
        if raw == NONE {
            return Err(value);
        }
        let entry = map(value);
        let index = self.reserve_index(token, raw, bytes, resident);
        let value = self.nodes[index.0.offset()].value.borrow_mut(token);
        value.0 = Some(entry);
        Ok(index)
    }

    pub(in crate::link::egress) fn reserve<U>(
        &self,
        token: &mut region::Token<'d>,
        value: U,
        bytes: usize,
        resident: usize,
    ) -> Result<indices::Reservation<'d, U>, U> {
        let raw = self.free.get();
        if raw == NONE {
            return Err(value);
        }
        let index = self.reserve_index(token, raw, bytes, resident);
        Ok(indices::Reservation::new(index, value))
    }

    fn reserve_index(
        &self,
        token: &mut region::Token<'d>,
        raw: u32,
        bytes: usize,
        resident: usize,
    ) -> indices::ReservedIndex<'d> {
        let slot = indices::Slot::<'d>::new(raw);
        let next = self.nodes[slot.offset()].metadata.borrow(token).next;
        self.free.set(next);
        {
            let metadata = self.nodes[slot.offset()].metadata.borrow_mut(token);
            metadata.next = NONE;
            metadata.bytes = bytes;
            metadata.resident = resident;
        }
        let value = self.nodes[slot.offset()].value.borrow_mut(token);
        debug_assert!(value.0.is_none());
        indices::ReservedIndex(slot)
    }

    fn take_node(
        &self,
        token: &mut region::Token<'d>,
        index: u32,
    ) -> Option<(u32, T, usize, usize)> {
        let slot = indices::Slot::<'d>::new(index);
        let value = self.nodes[slot.offset()].value.borrow_mut(token).0.take()?;
        let metadata = self.nodes[slot.offset()].metadata.borrow_mut(token);
        let next = metadata.next;
        let bytes = metadata.bytes;
        let resident = metadata.resident;
        metadata.bytes = 0;
        metadata.resident = 0;
        metadata.next = self.free.replace(index);
        Some((next, value, bytes, resident))
    }

    fn drain_nodes(&self, token: &mut region::Token<'d>, head: u32) {
        let mut drain = NodeDrain {
            pool: self,
            token,
            head,
        };
        while let Some(value) = drain.take() {
            drop(value);
        }
    }
}

struct NodeDrain<'a, 'd, T> {
    pool: &'a Pool<'d, T>,
    token: &'a mut region::Token<'d>,
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
