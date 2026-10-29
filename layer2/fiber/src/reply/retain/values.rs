use std::mem;

use crate::reply::retain::pool;

/// Owns retained values for one exact reply entry.
///
/// ```compile_fail
/// use dope_fiber::reply::retain::Values;
/// fn escape<'arena, 'credits, 'region>(credit: Values<'arena, 'credits, 'region>) -> Values<'static, 'credits, 'region> {
///     credit
/// }
/// ```
#[repr(transparent)]
pub struct Values<'arena, 'credits, 'region> {
    active: pool::Active<'arena, 'credits, 'region>,
}

impl<'arena, 'credits, 'region> Values<'arena, 'credits, 'region> {
    pub(in crate::reply::retain) const fn new(
        active: pool::Active<'arena, 'credits, 'region>,
    ) -> Self {
        Self { active }
    }
}

impl Drop for Values<'_, '_, '_> {
    fn drop(&mut self) {
        self.active.drop_values();
    }
}

const _: () = {
    assert!(mem::size_of::<Values<'static, 'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Values<'static, 'static, 'static>>>() == mem::size_of::<usize>());
};
