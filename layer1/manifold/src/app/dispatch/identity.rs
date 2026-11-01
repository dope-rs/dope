//! Connection identity storage.
use std::{cell, mem};

use dope_core::driver::route::indices;

/// Driver-lifetime-, route-, and family-branded connection identity.
///
/// Implementations project the fixed index while `Self` retains its full brand.
pub trait Identity: Copy + Eq {
    /// Projects the compact driver-issued slot while retaining `Self`'s brand.
    fn slot(self) -> indices::SlotIndex;
}

/// Result of claiming an empty identity binding.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Bind {
    Bound,
    Occupied,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Bound<I, T> {
    id: I,
    value: T,
}

/// One generation-checked binding and its owner-bound value.
#[repr(transparent)]
pub struct Binding<I: Identity, T: Copy = ()>(cell::Cell<Option<Bound<I, T>>>);

impl<I: Identity, T: Copy> Default for Binding<I, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: Identity, T: Copy> Binding<I, T> {
    pub const fn new() -> Self {
        Self(cell::Cell::new(None))
    }

    pub fn current(&self) -> Option<(I, T)> {
        self.0.get().map(|bound| (bound.id, bound.value))
    }

    pub fn bind(&self, id: I, value: T) -> Bind {
        if self.current().is_some() {
            Bind::Occupied
        } else {
            self.0.set(Some(Bound { id, value }));
            Bind::Bound
        }
    }

    pub fn clear(&self, id: I) {
        if self.0.get().map(|bound| bound.id) == Some(id) {
            self.0.set(None);
        }
    }

    pub fn replace_if(&self, id: I, from: T, to: T) -> Option<T>
    where
        T: PartialEq,
    {
        if self
            .0
            .get()
            .is_some_and(|bound| bound.id == id && bound.value == from)
        {
            self.0.set(Some(Bound { id, value: to }));
            Some(from)
        } else {
            None
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Bind>() == 1);
    assert!(!mem::needs_drop::<Bind>());
};
