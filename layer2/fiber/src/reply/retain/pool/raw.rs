use std::{cell, marker};

use o3::{depot, depot::arena::linked::fifo::ends};

use crate::reply::retain::grant::entries;

/// # Safety
/// Retain the original pool and grant as one affine owner; consume it only once.
pub(super) unsafe trait Release<'credits> {
    /// # Safety
    /// `grant` must be this pool's original still-counted entry acquisition.
    unsafe fn release_slot(&self, grant: entries::Grant<'credits, entries::Units<1, 0>>);
}

const END: u32 = u32::MAX - 1;
const VACANT: u32 = u32::MAX;

/// One chain paired permanently with its exact stable link projection.
/// # Safety
/// Stored and operation-proved identities map injectively to stable, non-reentrant links in this chain.
pub(super) unsafe trait Domain<I: depot::raw::Compact> {
    fn chain(&self) -> &ends::Cell<Slot<I>>;

    fn link(&self, index: I) -> &Link<I>;

    fn pop_front(&self) -> Option<I> {
        Some(pop(self, self.chain().front()?))
    }

    /// Publishes `index` as this chain's back unless its link is already linked.
    fn push_back(&self, index: I) {
        let link = self.link(index);
        if link.is_linked() {
            return;
        }
        link.begin();
        push(self, index);
    }
}

#[repr(C)]
pub(super) struct Word<I> {
    raw: u32,
    index: marker::PhantomData<fn(I) -> I>,
}

#[repr(C)]
pub(super) struct Slot<I> {
    raw: u32,
    index: marker::PhantomData<fn(I) -> I>,
}

#[repr(transparent)]
pub(super) struct Link<I>(cell::Cell<Slot<I>>);

pub(super) struct Round<I> {
    pub(super) chain: ends::Cell<Slot<I>>,
    through: cell::Cell<Slot<I>>,
    dirty: cell::Cell<bool>,
}

impl<I> Copy for Word<I> {}

impl<I> Clone for Word<I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I> Copy for Slot<I> {}

impl<I> Clone for Slot<I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I> PartialEq for Word<I> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}
impl<I> Eq for Word<I> {}
impl<I> PartialEq for Slot<I> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}
impl<I> Eq for Slot<I> {}

// SAFETY: END decodes only to None; every present word round-trips unchanged.
unsafe impl<I> depot::raw::Link for Slot<I> {
    type Item = Word<I>;
    const NONE: Self = Self::new(END);

    fn item(self) -> Option<Self::Item> {
        (self.raw != END).then_some(Word {
            raw: self.raw,
            index: marker::PhantomData,
        })
    }

    unsafe fn some(item: Self::Item) -> Self {
        Self::from_word(item)
    }
}

impl<I: depot::raw::Compact> Word<I> {
    fn new(index: I) -> Self {
        const { assert!(I::MAX < END, "bound") };
        Self {
            raw: index.into_raw(),
            index: marker::PhantomData,
        }
    }

    fn index(self) -> I {
        // SAFETY: Word retains the exact encoding produced by this compact domain.
        unsafe { I::from_raw(self.raw) }
    }
}

impl<I> Slot<I> {
    const NONE: Self = Self::new(END);
    const VACANT: Self = Self::new(VACANT);

    const fn new(raw: u32) -> Self {
        Self {
            raw,
            index: marker::PhantomData,
        }
    }

    const fn from_word(word: Word<I>) -> Self {
        Self::new(word.raw)
    }
}

impl<I> Link<I> {
    pub(super) const fn new() -> Self {
        Self(cell::Cell::new(Slot::VACANT))
    }

    pub(super) fn is_linked(&self) -> bool {
        self.0.get().raw != VACANT
    }

    fn begin(&self) {
        self.0.set(Slot::new(END));
    }

    fn append(&self, word: Word<I>) {
        self.0.set(Slot::from_word(word));
    }

    fn take(&self) -> Slot<I> {
        self.0.replace(Slot::VACANT)
    }
}

impl<I> Round<I> {
    pub(super) const fn new() -> Self {
        Self {
            chain: ends::Cell::empty(),
            through: cell::Cell::new(Slot::NONE),
            dirty: cell::Cell::new(false),
        }
    }

    pub(super) fn refund(&self) {
        if !self.chain.is_empty() {
            self.dirty.set(true);
        }
    }

    pub(super) fn has_pending(&self) -> bool {
        self.through.get() != Slot::NONE || self.dirty.get()
    }
}

impl<I: depot::raw::Compact> Round<I> {
    /// Advances one fair round over `domain`, the chain this round tracks.
    ///
    /// Both endpoints and links are read through `domain`, so a mispaired round
    /// can only skew the walk order, never detach a foreign link.
    pub(super) fn advance<D: Domain<I> + ?Sized>(&self, domain: &D) -> Option<I> {
        let chain = domain.chain();
        if self.through.get() == Slot::NONE {
            if !self.dirty.replace(false) {
                return None;
            }
            self.through
                .set(chain.back().map_or(Slot::NONE, Slot::from_word));
        }
        let front = chain.front()?;
        if Slot::from_word(front.get()) == self.through.get() {
            self.through.set(Slot::NONE);
        }
        Some(pop(domain, front))
    }
}

fn pop<I: depot::raw::Compact, D: Domain<I> + ?Sized>(
    domain: &D,
    front: ends::Front<&ends::Cell<Slot<I>>, Slot<I>>,
) -> I {
    // SAFETY: the caller retains this exact domain's present front. Link
    // projection cannot reenter, and taking it returns its original successor.
    unsafe {
        ends::Front::pop(depot::raw::Proof::new((front, (), |word, ()| {
            let index = word.index();
            (domain.link(index).take(), index)
        })))
    }
}

fn push<I: depot::raw::Compact, D: Domain<I> + ?Sized>(domain: &D, index: I) {
    let word = Word::new(index);
    let chain = domain.chain();
    // SAFETY: the operation owns one exact vacant identity; all links remain
    // stable and projection cannot reenter while the endpoints are published.
    unsafe {
        ends::Cell::push_back(depot::raw::Proof::new((chain, word, |previous| {
            if let Some(previous) = previous {
                domain.link(previous.index()).append(word);
            }
        })));
    }
}

const _: () = {
    assert!(std::mem::size_of::<Link<()>>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<ends::Cell<Slot<()>>>() == 2 * std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Round<()>>() == 4 * std::mem::size_of::<u32>());
};
