//! Clone-aware retained reply values.

use crate::reply::retain;

/// A value detached from its reply slot without releasing its resources.
pub struct Retained<'arena, 'credits, 'region, T> {
    pub(super) value: T,
    pub(super) held: retain::Held<'arena, 'credits, 'region>,
}

impl<'arena, 'credits, 'region, T> Retained<'arena, 'credits, 'region, T> {
    pub(crate) const fn new(value: T, held: retain::Held<'arena, 'credits, 'region>) -> Self {
        Self { value, held }
    }

    pub fn into_parts(self) -> (T, retain::Held<'arena, 'credits, 'region>) {
        (self.value, self.held)
    }
}
