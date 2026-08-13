use crate::driver::route;

/// Affine submission input paired with its exact target.
///
/// ```compile_fail
/// use dope_core::driver::route::{Bound, KeyTag};
///
/// fn cross_route<'d>(input: Bound<'d, KeyTag<1>, ()>) {
///     submit(input);
/// }
///
/// fn submit<'d>(_input: Bound<'d, KeyTag<2>, ()>) {}
/// ```
#[must_use = "bound submission input must be submitted or dropped"]
pub struct Bound<'d, Tag: route::Tag, T> {
    target: route::Target<'d, Tag>,
    value: T,
}

impl<'d, Tag: route::Tag, T> Bound<'d, Tag, T> {
    pub(super) const fn new(target: route::Target<'d, Tag>, value: T) -> Self {
        Self { target, value }
    }

    pub const fn target(&self) -> route::Target<'d, Tag> {
        self.target
    }

    pub const fn value(&self) -> &T {
        &self.value
    }

    pub(crate) fn into_parts(self) -> (route::Target<'d, Tag>, T) {
        (self.target, self.value)
    }
}
