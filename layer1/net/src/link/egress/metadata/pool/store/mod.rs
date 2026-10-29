use std::{cell, mem, num};

use dope_core::io::transfer;
use o3::{
    alloc,
    base::cell::region,
    buffer::{resident, segments::scatter},
    depot,
    depot::arena::linked::{fifo::ends, head},
};

use crate::link::egress::{self, data, load::credit, metadata::pool::store::index::Index};

pub(in crate::link::egress) mod index;
mod view;
pub(in crate::link::egress) use view::View;

/// Free-list cell without the unused FIFO plane required by `pool::Lanes`.
#[repr(C)]
struct Cell<'p, 'd, T> {
    value: mem::MaybeUninit<egress::NonEmpty<T>>,
    entry: mem::MaybeUninit<credit::Stored<'p>>,
    position: cell::Cell<scatter::raw::Position>,
    next: Option<Index<'d>>,
    occupied: bool,
}

impl<T> Drop for Cell<'_, '_, T> {
    fn drop(&mut self) {
        if self.occupied {
            // SAFETY: publication initializes both owners before setting occupied;
            // every detached or discarded cell clears it before moving either owner.
            let entry = unsafe { self.entry.assume_init_read() };
            entry.into_grant().release();
            unsafe { self.value.assume_init_drop() };
        }
    }
}

/// Values move only after their node has been detached from every chain.
impl<'p, T> Unpin for Cell<'p, '_, T> {}

struct Node<'p, 'd, T> {
    cell: region::Value<region::Identity<'d>, Cell<'p, 'd, T>>,
}

impl<'p, 'd, T> Node<'p, 'd, T> {
    const fn vacant(token: &region::Token<region::Identity<'d>>, next: Option<Index<'d>>) -> Self {
        Self {
            cell: token.bind(Cell {
                value: mem::MaybeUninit::uninit(),
                entry: mem::MaybeUninit::uninit(),
                position: cell::Cell::new(scatter::raw::Position::new()),
                next,
                occupied: false,
            }),
        }
    }

    fn reserve(&self, token: &mut region::Token<region::Identity<'d>>) -> Option<Index<'d>> {
        token.borrow_mut(&self.cell).next.take()
    }

    fn clear(&self, token: &mut region::Token<region::Identity<'d>>) {
        token.borrow_mut(&self.cell).next = None;
    }

    fn discard(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> (Option<Index<'d>>, credit::Stored<'p>) {
        let cell = token.borrow_mut(&self.cell);
        let next = cell.next.take();
        cell.occupied = false;
        unsafe {
            cell.value.assume_init_drop();
        }
        let stored = unsafe { cell.entry.assume_init_read() };
        (next, stored)
    }
}

pub(in crate::link::egress) struct Slab<'p, 'd, T> {
    nodes: Box<[Node<'p, 'd, T>]>,
    free: head::Cell<Option<Index<'d>>>,
}

pub(in crate::link::egress) struct Vacancy<'a, 'p, 'd, T> {
    slab: &'a Slab<'p, 'd, T>,
    front: head::Front<&'a head::Cell<Option<Index<'d>>>, Option<Index<'d>>>,
}

pub(in crate::link::egress) struct Reservation<'a, 'token, 'p, 'd, T> {
    vacancy: Vacancy<'a, 'p, 'd, T>,
    token: &'token mut region::Token<region::Identity<'d>>,
}

pub(in crate::link::egress) struct Pair<'a, 'token, 'p, 'd, T> {
    vacancy: Vacancy<'a, 'p, 'd, T>,
    second: Index<'d>,
    token: &'token mut region::Token<region::Identity<'d>>,
}

pub(in crate::link::egress) enum Pairing<'a, 'token, 'p, 'd, T> {
    One(&'token mut region::Token<region::Identity<'d>>),
    Two(Pair<'a, 'token, 'p, 'd, T>),
}

struct Free<'a, 'd> {
    token: &'a mut region::Token<region::Identity<'d>>,
    index: Index<'d>,
}

struct Second<'a, 'd> {
    token: &'a mut region::Token<region::Identity<'d>>,
    index: Index<'d>,
}

unsafe impl<'a, 'p, 'd, T>
    depot::raw::HeadPop<Option<Index<'d>>, &'a mut region::Token<region::Identity<'d>>>
    for &Slab<'p, 'd, T>
{
    type Output = Index<'d>;

    unsafe fn pop(
        self,
        index: Index<'d>,
        token: &'a mut region::Token<region::Identity<'d>>,
    ) -> (Option<Index<'d>>, Self::Output) {
        let next = self.node(index).reserve(token);
        (next, index)
    }
}

unsafe impl<'a, 'p, 'd, T> depot::raw::HeadPop<Option<Index<'d>>, Second<'a, 'd>>
    for &Slab<'p, 'd, T>
{
    type Output = (
        &'a mut region::Token<region::Identity<'d>>,
        Index<'d>,
        Index<'d>,
    );

    unsafe fn pop(
        self,
        index: Index<'d>,
        second: Second<'a, 'd>,
    ) -> (Option<Index<'d>>, Self::Output) {
        self.node(index).clear(second.token);
        let next = self.node(second.index).reserve(second.token);
        (next, (second.token, index, second.index))
    }
}

unsafe impl<'a, 'p, 'd, T> depot::raw::HeadPush<Option<Index<'d>>, Free<'a, 'd>>
    for &Slab<'p, 'd, T>
{
    unsafe fn push(self, free: Free<'a, 'd>, head: Option<Index<'d>>) -> Index<'d> {
        self.set_next(free.token, free.index, head);
        free.index
    }
}

impl<'p, 'd, T> Slab<'p, 'd, T> {
    fn node(&self, index: Index<'d>) -> &Node<'p, 'd, T> {
        // SAFETY: only this store constructs indices, within its allocated extent.
        // Reservations and lane chains retain their exact originating pool; neither
        // moving the boxed store nor changing a token changes that allocation.
        unsafe { self.nodes.get_unchecked(index.offset()) }
    }

    pub(in crate::link::egress::metadata) fn try_new(
        token: &region::Token<region::Identity<'d>>,
        len: egress::Entries,
    ) -> Result<Self, alloc::Error> {
        let raw = len.raw();
        let cap = raw as usize;
        Ok(Self {
            nodes: alloc::BoxSliceExt::try_from_fn(cap, |index| {
                let next = index as u32 + 1;
                let link = if next < raw {
                    // SAFETY: next < raw <= u32::MAX proves a represented successor.
                    let key = unsafe { num::NonZero::<u32>::new_unchecked(next + 1) };
                    Some(Index::new(key))
                } else {
                    None
                };
                Node::vacant(token, link)
            })?,
            free: head::Cell::new((raw != 0).then_some(Index::new(num::NonZero::<u32>::MIN))),
        })
    }

    pub(in crate::link::egress) fn vacancy(&self) -> Option<Vacancy<'_, 'p, 'd, T>> {
        let front = self.free.front()?;
        Some(Vacancy { slab: self, front })
    }

    pub(in crate::link::egress) fn reservation<'a, 'token>(
        &'a self,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> Result<Reservation<'a, 'token, 'p, 'd, T>, &'token mut region::Token<region::Identity<'d>>>
    {
        let Some(vacancy) = self.vacancy() else {
            return Err(token);
        };
        Ok(Reservation { vacancy, token })
    }

    pub(in crate::link::egress::metadata) fn write(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        slot: Index<'d>,
        value: egress::NonEmpty<T>,
        entry: credit::Stored<'p>,
    ) {
        let cell = token.borrow_mut(&self.node(slot).cell);
        cell.value = mem::MaybeUninit::new(value);
        cell.position.set(scatter::raw::Position::new());
        cell.entry = mem::MaybeUninit::new(entry);
        cell.occupied = true;
    }

    pub(in crate::link::egress::metadata) fn append(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        chain: &mut ends::Exclusive<Option<Index<'d>>>,
        slot: Index<'d>,
    ) {
        unsafe {
            ends::Exclusive::push_back(depot::raw::Proof::new((chain, slot, |prev| {
                self.set_next(token, slot, None);
                if let Some(prev) = prev {
                    self.set_next(token, prev, Some(slot));
                }
            })));
        }
    }

    pub(in crate::link::egress::metadata) fn get<'a>(
        &'a self,
        token: &'a region::Token<region::Identity<'d>>,
        slot: Index<'d>,
    ) -> View<'a, T> {
        let cell = token.borrow(&self.node(slot).cell);
        // SAFETY: a linked cell retains its initialized source and original credit.
        let (source, entry) =
            unsafe { (cell.value.assume_init_ref(), cell.entry.assume_init_ref()) };
        View {
            source,
            remaining: entry.count(),
            position: &cell.position,
        }
    }

    pub(in crate::link::egress::metadata) fn value<'a>(
        &'a self,
        token: &'a region::Token<region::Identity<'d>>,
        slot: Index<'d>,
    ) -> &'a egress::NonEmpty<T> {
        let cell = token.borrow(&self.node(slot).cell);
        unsafe { cell.value.assume_init_ref() }
    }

    pub(in crate::link::egress::metadata) fn next(
        &self,
        token: &region::Token<region::Identity<'d>>,
        slot: Index<'d>,
    ) -> Option<Index<'d>> {
        token.borrow(&self.node(slot).cell).next
    }

    pub(in crate::link::egress::metadata) fn detach(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        slot: Index<'d>,
    ) -> (Option<Index<'d>>, egress::NonEmpty<T>, credit::Stored<'p>) {
        let cell = token.borrow_mut(&self.node(slot).cell);
        let next = cell.next.take();
        cell.occupied = false;
        let value = unsafe { cell.value.assume_init_read() };
        let entry = unsafe { cell.entry.assume_init_read() };
        (next, value, entry)
    }

    pub(in crate::link::egress::metadata) fn consume(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        slot: Index<'d>,
        bytes: transfer::NonZero,
    ) -> credit::Prefix {
        let cell = token.borrow_mut(&self.node(slot).cell);
        // SAFETY: the retained lane prefix owns this initialized entry, and the
        // exclusive token prevents other access while its byte count changes.
        let stored = unsafe { cell.entry.assume_init_mut() };
        stored.consume(transfer::Len::from_nonzero(bytes))
    }

    /// Restores the detached front's original owner to its reserved slot,
    /// preserving the checkpoint across partial completions.
    pub(in crate::link::egress::metadata) fn restore(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        slot: Index<'d>,
        value: egress::NonEmpty<T>,
        next: Option<Index<'d>>,
        entry: credit::Stored<'p>,
    ) {
        let cell = token.borrow_mut(&self.node(slot).cell);
        cell.next = next;
        cell.value = mem::MaybeUninit::new(value);
        cell.entry = mem::MaybeUninit::new(entry);
        cell.occupied = true;
    }

    pub(in crate::link::egress::metadata) fn recycle(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        slot: Index<'d>,
    ) {
        let free = Free { token, index: slot };
        // SAFETY: `slot` is detached and branded to this slab and token.
        unsafe { depot::raw::HeadPush::apply(self, &self.free, free) }
    }

    pub(in crate::link::egress::metadata) fn discard(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        chain: &mut ends::Exclusive<Option<Index<'d>>>,
    ) -> Option<credit::Stored<'p>> {
        let front = chain.front()?;
        // SAFETY: this chain owns the initialized node and its original credit.
        Some(unsafe {
            ends::Front::pop(depot::raw::Proof::new((front, (), |slot, ()| {
                let (next, stored) = self.node(slot).discard(token);
                self.recycle(token, slot);
                (next, stored)
            })))
        })
    }

    pub(in crate::link::egress::metadata) fn set_next(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        slot: Index<'d>,
        next: Option<Index<'d>>,
    ) {
        token.borrow_mut(&self.node(slot).cell).next = next;
    }
}

impl<'a, 'p, 'd, T> Vacancy<'a, 'p, 'd, T> {
    pub(in crate::link::egress::metadata) fn reserve<V, P>(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        proof: P,
        value: V,
    ) -> (Index<'d>, P, V) {
        // SAFETY: this vacancy binds the front to this slab and driver token.
        let index = unsafe { depot::raw::HeadPop::apply(self.slab, self.front, token) };
        (index, proof, value)
    }
}

impl<'a, 'token, 'p, 'd, T> Reservation<'a, 'token, 'p, 'd, T> {
    pub(in crate::link::egress) fn pair(self) -> Pairing<'a, 'token, 'p, 'd, T> {
        let Self { vacancy, token } = self;
        let first = vacancy.front.item();
        let Some(second) = vacancy.slab.next(token, first) else {
            return Pairing::One(token);
        };
        Pairing::Two(Pair {
            vacancy,
            second,
            token,
        })
    }

    pub(in crate::link::egress) fn reserve<P, V>(
        self,
        proof: P,
        value: V,
    ) -> (
        &'token mut region::Token<region::Identity<'d>>,
        Index<'d>,
        P,
        V,
    ) {
        let Self { vacancy, token } = self;
        let (index, proof, value) = vacancy.reserve(token, proof, value);
        (token, index, proof, value)
    }
}

impl<'a, 'token, 'p, 'd, T> Pair<'a, 'token, 'p, 'd, T> {
    pub(in crate::link::egress) fn reserve<P, Q, V, W>(
        self,
        first: (P, V),
        second: (Q, W),
    ) -> (
        &'token mut region::Token<region::Identity<'d>>,
        Reserved<'d, P, V>,
        Reserved<'d, Q, W>,
    ) {
        let Self {
            vacancy,
            second: sec,
            token,
        } = self;
        let (proof, value) = first;
        let (other, data) = second;
        let link = Second { token, index: sec };
        // SAFETY: `vacancy` and `link` bind both detached links to this slab.
        let (token, idx, sec) =
            unsafe { depot::raw::HeadPop::apply(vacancy.slab, vacancy.front, link) };
        (token, (idx, proof, value), (sec, other, data))
    }
}

const _: () = {
    assert!(usize::BITS >= u32::BITS);
    assert!(
        mem::size_of::<region::Value<region::Identity<'static>, Cell<'static, 'static, ()>>>()
            == mem::size_of::<Cell<'static, 'static, ()>>()
    );
};
const _: () =
    assert!(mem::size_of::<head::Cell<Option<Index<'static>>>>() == mem::size_of::<u32>());

const _: () = {
    let tail = mem::size_of::<credit::Stored<'_>>()
        + mem::size_of::<[u32; 2]>()
        + mem::size_of::<scatter::raw::Position>();
    assert!(
        mem::size_of::<Cell<'_, '_, &[u8]>>() == mem::size_of::<egress::NonEmpty<&[u8]>>() + tail
    );
    assert!(
        mem::size_of::<Cell<'_, '_, resident::Shared<'_>>>()
            == mem::size_of::<egress::NonEmpty<resident::Shared<'_>>>() + tail
    );
    assert!(
        mem::size_of::<Cell<'_, '_, data::compose::Tagged<resident::Shared<'_>, u64>>>()
            == mem::size_of::<egress::NonEmpty<data::compose::Tagged<resident::Shared<'_>, u64>>>()
                + tail
    );
};

type Reserved<'d, P, V> = (Index<'d>, P, V);
