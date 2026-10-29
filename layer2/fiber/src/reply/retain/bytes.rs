use std::mem;

use o3::mem::rc;

use crate::reply::retain::pool;

/// Owns retained bytes for one exact reply entry.
///
/// ```compile_fail
/// use dope_fiber::reply::retain::Bytes;
/// fn escape<'arena, 'credits, 'region>(credit: Bytes<'arena, 'credits, 'region>) -> Bytes<'static, 'credits, 'region> {
///     credit
/// }
/// ```
#[repr(transparent)]
pub struct Bytes<'arena, 'credits, 'region> {
    active: pool::Active<'arena, 'credits, 'region>,
}

impl<'arena, 'credits, 'region> Bytes<'arena, 'credits, 'region> {
    pub(in crate::reply::retain) const fn new(
        active: pool::Active<'arena, 'credits, 'region>,
    ) -> Self {
        Self { active }
    }

    pub fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self {
            active: self.active.try_share()?,
        })
    }
}

impl Drop for Bytes<'_, '_, '_> {
    fn drop(&mut self) {
        self.active.drop_bytes();
    }
}

const _: () = {
    assert!(mem::size_of::<Bytes<'static, 'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Bytes<'static, 'static, 'static>>>() == mem::size_of::<usize>());
};
