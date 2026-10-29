use std::{cell, marker, mem, num};

use bitmap::{Access as _, entry::Row};
use o3::{
    base::marker::ThreadBound,
    depot::{self, index::bitmap},
};

use crate::{
    driver,
    driver::{route, route::lease},
};

const SHAPE: bitmap::Shape<num::NonZero<u32>, u32> = bitmap::Shape::<num::NonZero<u32>, u32>::new(
    num::NonZero::<u32>::MIN.saturating_add(route::CAPACITY as u32 - 1),
);
const WORDS: usize = 9;

pub(in crate::driver) struct Pending {
    state: (cell::UnsafeCell<State>, ThreadBound),
}

struct State {
    words: [bitmap::Word<u32>; WORDS],
    cursor: u8,
}

pub(in crate::driver) struct Retired<'d> {
    id: route::Id,
    driver: marker::PhantomData<driver::Reference<'d>>,
}

impl<'d> Retired<'d> {
    pub(in crate::driver::route) fn reserved<B: route::App>(
        _claim: lease::Reserved<'d, B>,
    ) -> Self {
        Self {
            id: <B as route::App>::ID,
            driver: marker::PhantomData,
        }
    }

    pub(in crate::driver::route) fn into_id(self) -> route::Id {
        let Self { id, driver: _ } = self;
        id
    }
}

impl Pending {
    pub(in crate::driver) const fn new() -> Self {
        Self {
            state: (
                cell::UnsafeCell::new(State {
                    words: [const { bitmap::Word::zero() }; WORDS],
                    cursor: 0,
                }),
                ThreadBound::NEW,
            ),
        }
    }

    pub(in crate::driver::route) fn defer<B: route::App>(reserved: lease::Reserved<'_, B>) {
        let queue = &reserved.driver().shared.context().retire;
        // SAFETY: the queue is thread-bound. This complete transition calls
        // no user code, and no state borrow escapes a queue operation.
        let state = unsafe { &mut *queue.state.0.get() };
        state.insert(reserved);
    }

    pub(in crate::driver) fn pop(driver: driver::Reference<'_>) -> Option<Retired<'_>> {
        let queue = &driver.shared.context().retire;
        // SAFETY: the thread-bound queue lends its state only for this
        // non-reentrant operation; an entry cannot outlive the call.
        let state = unsafe { &mut *queue.state.0.get() };
        let id = state.pop()?;
        Some(Retired {
            id,
            driver: marker::PhantomData,
        })
    }

    pub(in crate::driver) fn has_pending(&self) -> bool {
        // SAFETY: state transitions never call user code or expose their
        // exclusive borrow, and ThreadBound prevents concurrent access.
        unsafe { !(&*self.state.0.get()).is_empty(()) }
    }
}

// SAFETY: State retains eight initialized u32 leaves and their root summary.
// Only bitmap operations mutate them during one exclusive queue transition;
// the fixed u8 route domain fills all eight leaves without tail bits.
unsafe impl depot::raw::Words<u32> for State {
    type Plane = ();
    type Size = num::NonZero<u32>;

    fn shape(&self) -> bitmap::Shape<Self::Size, u32> {
        SHAPE
    }

    fn words(&self, (): ()) -> &[bitmap::Word<u32>] {
        &self.words
    }
}

// SAFETY: the thread-bound queue exposes no plane access during &mut State.
unsafe impl depot::raw::Bitmap<u32> for State {}

impl State {
    fn insert<B: route::App>(&mut self, _claim: lease::Reserved<'_, B>) {
        let raw = usize::from(B::ROUTE.raw());
        Row::clamp(self, raw).update::<true>(());
    }

    fn pop(&mut self) -> Option<route::Id> {
        let start = usize::from(self.cursor);
        let removed = (&mut *self).pop_from((), start)?;
        let index = removed.index() as u8;
        self.cursor = index.wrapping_add(1);
        Some(route::Id(index))
    }
}

const _: () = {
    assert!(u8::MAX as usize + 1 == route::CAPACITY);
    assert!(mem::size_of::<Pending>() == 40);
    assert!(mem::size_of::<Retired<'_>>() == mem::size_of::<route::Id>());
};
