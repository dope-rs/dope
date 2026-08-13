//! Common zero-cost connection identity storage.

use std::cell;

mod brand;

pub(crate) use brand::Brand;

/// Driver-lifetime-, route-, and family-branded connection identity.
///
/// Implementations project the fixed index while `Self` retains its full brand.
pub trait Identity: Brand + Copy + Eq {
    fn index(self) -> usize;
}

/// One generation-checked binding stored without erasing its identity brand.
#[repr(transparent)]
pub struct Binding<I: Identity>(cell::Cell<Option<I>>);

impl<I: Identity> Binding<I> {
    pub const fn new() -> Self {
        Self(cell::Cell::new(None))
    }

    pub fn current(&self) -> Option<I> {
        self.0.get()
    }

    pub fn matches(&self, id: I) -> bool {
        self.current() == Some(id)
    }

    pub fn try_bind(&self, id: I) -> bool {
        if self.current().is_some() {
            return false;
        }
        self.0.set(Some(id));
        true
    }

    pub fn replace(&self, id: I) -> Option<I> {
        self.0.replace(Some(id))
    }

    pub fn clear(&self, id: I) -> bool {
        if !self.matches(id) {
            return false;
        }
        self.0.set(None);
        true
    }
}

impl<I: Identity> Default for Binding<I> {
    fn default() -> Self {
        Self::new()
    }
}
