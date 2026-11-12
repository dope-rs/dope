use std::{cell, mem, num};

use dope_core::io::transfer;
use o3::{
    alloc,
    base::cell::region,
    buffer::resident,
    depot,
    depot::arena::linked::{fifo::ends, head},
};

use crate::link::egress::{self, data, load::credit, metadata::pool::store::index::Index};

pub(in crate::link::egress) mod index;
mod view;
pub(in crate::link::egress) use view::View;

/// Free-list cell without the unused FIFO plane required by `pool::Lanes`.
#[repr(C)]
struct Cell<'p, 'd, T, P: data::position::State = ()> {
    value: mem::MaybeUninit<egress::NonEmpty<T>>,
    entry: mem::MaybeUninit<credit::Stored<'p>>,
    position: mem::MaybeUninit<cell::Cell<P>>,
    next: Option<Index<'d>>,
    occupied: bool,
}

impl<T, P: data::position::State> Drop for Cell<'_, '_, T, P> {
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
impl<'p, T, P: data::position::State> Unpin for Cell<'p, '_, T, P> {}

struct Node<'p, 'd, T, P: data::position::State = ()> {
    cell: region::Value<region::Identity<'d>, Cell<'p, 'd, T, P>>,
}

impl<'p, 'd, T, P: data::position::State> Node<'p, 'd, T, P> {
    const fn vacant(token: &region::Token<region::Identity<'d>>, next: Option<Index<'d>>) -> Self {
        Self {
            cell: token.bind(Cell {
                value: mem::MaybeUninit::uninit(),
                entry: mem::MaybeUninit::uninit(),
                position: mem::MaybeUninit::uninit(),
                next,
                occupied: false,
            }),
        }
    }

    fn discard(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> (Option<Index<'d>>, credit::Stored<'p>) {
        let cell = token.borrow_mut(&self.cell);
        let next = cell.next;
        cell.occupied = false;
        unsafe {
            cell.value.assume_init_drop();
        }
        let stored = unsafe { cell.entry.assume_init_read() };
        (next, stored)
    }
}

pub(in crate::link::egress) struct Slab<'p, 'd, T, P: data::position::State = ()> {
    nodes: Box<[Node<'p, 'd, T, P>]>,
    free: head::Cell<Option<Index<'d>>>,
}

pub(in crate::link::egress) struct Vacancy<'a, 'p, 'd, T, P: data::position::State = ()> {
    slab: &'a Slab<'p, 'd, T, P>,
    front: head::Front<'a, Option<Index<'d>>>,
}

pub(in crate::link::egress) struct Reservation<'a, 'token, 'p, 'd, T, P: data::position::State = ()>
{
    vacancy: Vacancy<'a, 'p, 'd, T, P>,
    token: &'token mut region::Token<region::Identity<'d>>,
}

pub(in crate::link::egress) struct Pair<'a, 'token, 'p, 'd, T, P: data::position::State = ()> {
    vacancy: Vacancy<'a, 'p, 'd, T, P>,
    second: Index<'d>,
    token: &'token mut region::Token<region::Identity<'d>>,
}

pub(in crate::link::egress) enum Pairing<'a, 'token, 'p, 'd, T, P: data::position::State = ()> {
    One(&'token mut region::Token<region::Identity<'d>>),
    Two(Pair<'a, 'token, 'p, 'd, T, P>),
}

struct Free<'a, 'd> {
    token: &'a mut region::Token<region::Identity<'d>>,
    index: Index<'d>,
}

struct Second<'a, 'd> {
    token: &'a mut region::Token<region::Identity<'d>>,
    index: Index<'d>,
}

unsafe impl<'a, 'p, 'd, T, P: data::position::State>
    depot::raw::HeadPop<Option<Index<'d>>, &'a mut region::Token<region::Identity<'d>>>
    for &Slab<'p, 'd, T, P>
{
    type Output = Index<'d>;

    unsafe fn pop(
        self,
        index: Index<'d>,
        token: &'a mut region::Token<region::Identity<'d>>,
    ) -> (Option<Index<'d>>, Self::Output) {
        let next = self.next(token, index);
        (next, index)
    }
}

unsafe impl<'a, 'p, 'd, T, P: data::position::State>
    depot::raw::HeadPop<Option<Index<'d>>, Second<'a, 'd>> for &Slab<'p, 'd, T, P>
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
        let next = self.next(second.token, second.index);
        (next, (second.token, index, second.index))
    }
}

unsafe impl<'a, 'p, 'd, T, P: data::position::State>
    depot::raw::HeadPush<Option<Index<'d>>, Free<'a, 'd>> for &Slab<'p, 'd, T, P>
{
    unsafe fn push(self, free: Free<'a, 'd>, head: Option<Index<'d>>) -> Index<'d> {
        self.set_next(free.token, free.index, head);
        free.index
    }
}

impl<'p, 'd, T, P: data::position::State> Slab<'p, 'd, T, P> {
    fn node(&self, index: Index<'d>) -> &Node<'p, 'd, T, P> {
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

    pub(in crate::link::egress) fn vacancy(&self) -> Option<Vacancy<'_, 'p, 'd, T, P>> {
        let front = self.free.front()?;
        Some(Vacancy { slab: self, front })
    }

    pub(in crate::link::egress) fn reservation<'a, 'token>(
        &'a self,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> Result<
        Reservation<'a, 'token, 'p, 'd, T, P>,
        &'token mut region::Token<region::Identity<'d>>,
    > {
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
        cell.position = mem::MaybeUninit::new(cell::Cell::new(P::default()));
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
    ) -> View<'a, T, P> {
        let cell = token.borrow(&self.node(slot).cell);
        // SAFETY: publication initializes the source, checkpoint, and credit
        // before linking; detach and restore preserve that same owner.
        let (source, entry, position) = unsafe {
            (
                cell.value.assume_init_ref(),
                cell.entry.assume_init_ref(),
                cell.position.assume_init_ref(),
            )
        };
        View {
            source,
            remaining: entry.count(),
            position,
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
        let next = cell.next;
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

impl<'a, 'p, 'd, T, P: data::position::State> Vacancy<'a, 'p, 'd, T, P> {
    pub(in crate::link::egress::metadata) fn reserve(
        self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Index<'d> {
        // SAFETY: this vacancy binds the front to this slab and driver token.
        unsafe { depot::raw::HeadPop::apply(self.slab, self.front, token) }
    }
}

impl<'a, 'token, 'p, 'd, T, P: data::position::State> Reservation<'a, 'token, 'p, 'd, T, P> {
    pub(in crate::link::egress) fn pair(self) -> Pairing<'a, 'token, 'p, 'd, T, P> {
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

    pub(in crate::link::egress) fn reserve(
        self,
    ) -> (&'token mut region::Token<region::Identity<'d>>, Index<'d>) {
        let Self { vacancy, token } = self;
        let index = vacancy.reserve(token);
        (token, index)
    }
}

impl<'a, 'token, 'p, 'd, T, P: data::position::State> Pair<'a, 'token, 'p, 'd, T, P> {
    pub(in crate::link::egress) fn reserve(
        self,
    ) -> (
        &'token mut region::Token<region::Identity<'d>>,
        Index<'d>,
        Index<'d>,
    ) {
        let Self {
            vacancy,
            second,
            token,
        } = self;
        let link = Second {
            token,
            index: second,
        };
        // SAFETY: `vacancy` and `link` bind both detached links to this slab.
        unsafe { depot::raw::HeadPop::apply(vacancy.slab, vacancy.front, link) }
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
    let tail = mem::size_of::<credit::Stored<'_>>() + mem::size_of::<[u32; 2]>();
    assert!(
        mem::size_of::<Cell<'_, '_, &[u8], data::position::Saved>>()
            == mem::size_of::<Cell<'_, '_, &[u8]>>() + mem::size_of::<data::position::Saved>()
    );
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
