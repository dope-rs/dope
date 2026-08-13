use std::{marker, mem};

use o3::cell::region;

use crate::link::egress::metadata::pool;

const NONE: u32 = u32::MAX;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Slot<'d>(u32, marker::PhantomData<fn(&'d ()) -> &'d ()>);

impl<'d> Slot<'d> {
    pub(super) const NONE: Self = Self(NONE, marker::PhantomData);

    pub(super) const fn new(raw: u32) -> Self {
        Self(raw, marker::PhantomData)
    }

    pub(super) const fn raw(self) -> u32 {
        self.0
    }

    pub(super) const fn offset(self) -> usize {
        self.0 as usize
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::link::egress) struct ReservedIndex<'d>(pub(super) Slot<'d>);

#[must_use]
pub(in crate::link::egress) struct Reservation<'d, T> {
    index: ReservedIndex<'d>,
    value: T,
}

const _: () = assert!(mem::size_of::<Reservation<'static, ()>>() == mem::size_of::<u32>());
const _: () = assert!(mem::size_of::<ReservedIndex<'static>>() == mem::size_of::<u32>());
const _: () = assert!(mem::size_of::<LinkedIndex<'static>>() == mem::size_of::<u32>());
const _: () = assert!(mem::size_of::<DetachedIndex<'static>>() == mem::size_of::<u32>());

impl<'d, T> Reservation<'d, T> {
    pub(super) const fn new(index: ReservedIndex<'d>, value: T) -> Self {
        Self { index, value }
    }

    pub(in crate::link::egress) fn install<U>(
        self,
        pool: &pool::Pool<'d, U>,
        token: &mut region::Token<'d>,
        map: impl FnOnce(T) -> U,
    ) -> ReservedIndex<'d> {
        let Self { index, value } = self;
        pool.nodes[index.0.offset()].value.borrow_mut(token).0 = Some(map(value));
        index
    }

    pub(in crate::link::egress) fn rollback<U>(
        self,
        pool: &pool::Pool<'d, U>,
        token: &mut region::Token<'d>,
    ) -> T {
        let Self { index, value } = self;
        let metadata = pool.nodes[index.0.offset()].metadata.borrow_mut(token);
        metadata.bytes = 0;
        metadata.resident = 0;
        metadata.next = pool.free.replace(index.0.raw());
        value
    }
}

impl<'d> ReservedIndex<'d> {
    pub(in crate::link::egress) const NONE: Self = Self(Slot::NONE);

    pub(in crate::link::egress) fn is_none(self) -> bool {
        self == Self::NONE
    }

    pub(in crate::link::egress::metadata) fn into_linked(self) -> LinkedIndex<'d> {
        debug_assert!(!self.is_none());
        LinkedIndex(self.0)
    }

    pub(in crate::link::egress) fn set_next<T>(
        self,
        pool: &pool::Pool<'d, T>,
        token: &mut region::Token<'d>,
        next: Self,
    ) {
        pool.nodes[self.0.offset()].metadata.borrow_mut(token).next = next.0.raw();
    }

    pub(in crate::link::egress) fn drain<T>(
        &mut self,
        pool: &pool::Pool<'d, T>,
        token: &mut region::Token<'d>,
    ) {
        pool.drain_nodes(token, mem::replace(self, Self::NONE).0.raw());
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::link::egress) struct LinkedIndex<'d>(pub(super) Slot<'d>);

impl<'d> LinkedIndex<'d> {
    pub(in crate::link::egress) const NONE: Self = Self(Slot::NONE);

    pub(in crate::link::egress) fn is_none(self) -> bool {
        self == Self::NONE
    }

    pub(in crate::link::egress) fn inspect<T, R>(
        self,
        pool: &pool::Pool<'d, T>,
        token: &region::Token<'d>,
        inspect: impl FnOnce(&T) -> R,
    ) -> Option<R> {
        pool.nodes[self.0.offset()]
            .value
            .borrow(token)
            .0
            .as_ref()
            .map(inspect)
    }

    pub(in crate::link::egress::metadata) fn set_next<T>(
        self,
        pool: &pool::Pool<'d, T>,
        token: &mut region::Token<'d>,
        next: Self,
    ) {
        pool.nodes[self.0.offset()].metadata.borrow_mut(token).next = next.0.raw();
    }

    pub(in crate::link::egress) fn next<T>(
        self,
        pool: &pool::Pool<'d, T>,
        token: &region::Token<'d>,
    ) -> Self {
        Self(Slot::new(
            pool.nodes[self.0.offset()].metadata.borrow(token).next,
        ))
    }

    pub(in crate::link::egress::metadata) fn detach<T>(
        self,
        pool: &pool::Pool<'d, T>,
        token: &mut region::Token<'d>,
    ) -> Option<(Self, T, usize, usize, DetachedIndex<'d>)> {
        let value = pool.nodes[self.0.offset()]
            .value
            .borrow_mut(token)
            .0
            .take()?;
        let metadata = pool.nodes[self.0.offset()].metadata.borrow_mut(token);
        let next = Self(Slot::new(metadata.next));
        let bytes = metadata.bytes;
        let resident = metadata.resident;
        metadata.next = NONE;
        Some((next, value, bytes, resident, DetachedIndex(self.0)))
    }

    pub(in crate::link::egress::metadata) fn consume<T>(
        self,
        pool: &pool::Pool<'d, T>,
        token: &mut region::Token<'d>,
        bytes: usize,
    ) {
        let metadata = pool.nodes[self.0.offset()].metadata.borrow_mut(token);
        debug_assert!(metadata.bytes >= bytes);
        metadata.bytes -= bytes;
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(in crate::link::egress::metadata) struct DetachedIndex<'d>(Slot<'d>);

impl<'d> DetachedIndex<'d> {
    pub(in crate::link::egress::metadata) fn restore<T>(
        self,
        pool: &pool::Pool<'d, T>,
        token: &mut region::Token<'d>,
        value: T,
        next: LinkedIndex<'d>,
        bytes: usize,
        resident: usize,
    ) -> LinkedIndex<'d> {
        let stored = pool.nodes[self.0.offset()].value.borrow_mut(token);
        debug_assert!(stored.0.is_none());
        stored.0 = Some(value);
        let metadata = pool.nodes[self.0.offset()].metadata.borrow_mut(token);
        metadata.next = next.0.raw();
        metadata.bytes = bytes;
        metadata.resident = resident;
        LinkedIndex(self.0)
    }

    pub(in crate::link::egress::metadata) fn release<T>(
        self,
        pool: &pool::Pool<'d, T>,
        token: &mut region::Token<'d>,
    ) {
        debug_assert!(pool.nodes[self.0.offset()].value.borrow(token).0.is_none());
        let metadata = pool.nodes[self.0.offset()].metadata.borrow_mut(token);
        metadata.bytes = 0;
        metadata.resident = 0;
        metadata.next = pool.free.replace(self.0.raw());
    }
}
