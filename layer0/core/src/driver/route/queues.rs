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
    state: State,
    _thread: ThreadBound,
}

struct State {
    words: [bitmap::Word<u32>; WORDS],
    cursor: cell::Cell<u8>,
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
            state: State {
                words: [const { bitmap::Word::zero() }; WORDS],
                cursor: cell::Cell::new(0),
            },
            _thread: ThreadBound::NEW,
        }
    }

    pub(in crate::driver::route) fn defer<B: route::App>(reserved: lease::Reserved<'_, B>) {
        let state = &reserved.driver().shared.context().retire.state;
        Row::clamp(state, usize::from(B::ROUTE.raw())).update::<true>(());
    }

    pub(in crate::driver) fn pop(driver: driver::Reference<'_>) -> Option<Retired<'_>> {
        let state = &driver.shared.context().retire.state;
        let removed = state.pop_from((), usize::from(state.cursor.get()))?;
        let index = removed.index() as u8;
        state.cursor.set(index.wrapping_add(1));
        Some(Retired {
            id: route::Id(index),
            driver: marker::PhantomData,
        })
    }

    pub(in crate::driver) fn has_pending(&self) -> bool {
        !self.state.is_empty(())
    }
}

// SAFETY: State retains eight initialized u32 leaves and their root summary.
// Only bitmap operations mutate them inside Pending, without callbacks or reentry;
// ThreadBound excludes concurrent access. The u8 domain fills every leaf without tails.
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

const _: () = {
    assert!(u8::MAX as usize + 1 == route::CAPACITY);
    assert!(mem::size_of::<Pending>() == 40);
    assert!(mem::size_of::<Retired<'_>>() == mem::size_of::<route::Id>());
};
