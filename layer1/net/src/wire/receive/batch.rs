use std::{marker, num};

use dope_core::driver::schedule::app::batch;
use o3::mem;

use crate::wire::{self, receive};

/// Scheduler quota remaining after the implicit first batch item.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Spare(usize);

impl Spare {
    pub const fn from_usize(value: usize) -> Self {
        Self(value)
    }

    pub(super) fn fit(
        self,
        max: num::NonZero<usize>,
        min: num::NonZero<usize>,
    ) -> Option<num::NonZero<usize>> {
        let spare = self.0.min(max.get() - 1);
        let items = num::NonZero::<usize>::MIN.saturating_add(spare);
        (items >= min).then_some(items)
    }
}

/// Exact wire-produced receive batch with no representation tag.
#[repr(transparent)]
pub struct Items<'input, W: wire::Spec + 'input> {
    pub(super) inner: <W::Receive as receive::Policy<W>>::Batch<'input>,
}

const _: () = assert!(std::mem::size_of::<Spare>() == std::mem::size_of::<usize>());

impl<'input, W: wire::Spec + 'input> Items<'input, W> {
    pub(crate) fn from_raw(inner: <W::Receive as receive::Policy<W>>::Batch<'input>) -> Self {
        Self { inner }
    }
}

/// At most one receive item retained by its optional owner.
#[must_use = "a live batch item must be consumed"]
#[repr(transparent)]
pub struct One<T> {
    pub(super) item: Option<T>,
}

impl<'input, W: wire::Spec + 'input> Iterator for Items<'input, W> {
    type Item =
        <<W::Receive as receive::Policy<W>>::Batch<'input> as receive::raw::Batch<'input>>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        receive::raw::Batch::next(&mut self.inner)
    }
}

/// Per-call batch capacity branded by its wire policy.
pub struct Capacity<'d, W: wire::Spec> {
    items: num::NonZero<usize>,
    wire: marker::PhantomData<fn(&'d ()) -> Binding<'d, W>>,
}

impl<'d, W: wire::Spec + 'd> Capacity<'d, W> {
    const MAX: num::NonZero<usize> = {
        let max =
            <<W::Receive as receive::Policy<W>>::Batch<'d> as receive::raw::Batch<'d>>::MAX_ITEMS;
        let min = <<W::Receive as receive::Policy<W>>::Batch<'d> as receive::raw::Batch<
            'd,
        >>::MIN_CAPACITY;
        assert!(min.get() <= max.get());
        assert!(max.get() <= 32);
        max
    };

    const MIN: num::NonZero<usize> =
        <<W::Receive as receive::Policy<W>>::Batch<'d> as receive::raw::Batch<'d>>::MIN_CAPACITY;

    /// Fits a batch to the quota available beyond its implicit first item.
    pub fn fit_spare(spare: Spare) -> Option<Self> {
        let items = spare.fit(Self::MAX, Self::MIN)?;
        Some(Self {
            items,
            wire: marker::PhantomData,
        })
    }

    pub fn full() -> Self {
        Self {
            items: Self::MAX,
            wire: marker::PhantomData,
        }
    }
}

impl<W: wire::Spec> Capacity<'_, W> {
    pub const fn items(&self) -> num::NonZero<usize> {
        self.items
    }
}

/// Exclusive admission for the output of one receive operation.
pub struct Grant<'id, 'd, W: wire::Spec> {
    bound: batch::Bound<'id, Capacity<'d, W>>,
}

/// The output and its exact reservation settlement travel together.
#[must_use]
pub struct Produced<'id, B> {
    output: B,
    unused: mem::raw::Unused<'id>,
}

impl<'id, 'd, W: wire::Spec> Grant<'id, 'd, W> {
    pub(crate) fn new(bound: batch::Bound<'id, Capacity<'d, W>>) -> Self {
        Self { bound }
    }

    pub fn items(&self) -> num::NonZero<usize> {
        self.bound.get().items()
    }

    /// Produces exactly one item, which every nonzero admission permits.
    pub fn one<T>(self, item: T) -> Produced<'id, One<T>> {
        self.maybe(Some(item))
    }

    /// Produces at most one item under the implicit first-item admission.
    pub fn maybe<T>(self, item: Option<T>) -> Produced<'id, One<T>> {
        let output = One { item };
        Produced {
            output,
            unused: self.bound.empty(),
        }
    }

    /// Runs a bounded producer under this admission and preserves its settlement.
    pub fn produce<'input, P>(
        self,
        producer: P,
    ) -> Produced<'id, <W::Receive as receive::Policy<W>>::Batch<'input>>
    where
        W: 'input,
        P: receive::raw::Producer<'input, W>,
    {
        use batch::raw::Settle as _;

        let output = producer.produce(self.bound.get());
        let used = <<W::Receive as receive::Policy<W>>::Batch<'input> as receive::raw::Batch<
            'input,
        >>::len(&output);
        // SAFETY: Producer constructs output under a borrow of this bound's
        // exact capacity. It cannot replace the admission retained here.
        let unused = unsafe { self.bound.take(used) };
        Produced { output, unused }
    }
}

impl<'id, B> Produced<'id, B> {
    pub(crate) fn into_parts(self) -> (B, mem::raw::Unused<'id>) {
        (self.output, self.unused)
    }
}

const _: () = {
    assert!(std::mem::size_of::<One<u8>>() == std::mem::size_of::<Option<u8>>());
    assert!(std::mem::align_of::<One<u8>>() == std::mem::align_of::<Option<u8>>());
};

// SAFETY: `items` is immutable and is the sole runtime representation of the
// capacity while an application batch reservation owns the value.
unsafe impl<'d, W: wire::Spec> batch::raw::Limit for Capacity<'d, W> {
    fn get(&self) -> num::NonZero<usize> {
        self.items
    }
}

type Binding<'d, W> = (&'d (), W);
