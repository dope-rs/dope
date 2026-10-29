use o3::alloc;
use table::borrows;

use crate::driver::{route, route::table};

#[repr(transparent)]
pub struct Slab<T, Tag: route::Tag> {
    inner: table::Storage<T, Tag>,
}

impl<T, Tag: route::Tag> Slab<T, Tag> {
    /// Reserves the complete typed target table transactionally.
    pub fn try_new(cap: table::Capacity) -> Result<Self, alloc::Error> {
        Ok(Self {
            inner: table::Storage::try_new(cap.0)?,
        })
    }

    pub fn view(&self) -> borrows::View<'_, T, Tag> {
        borrows::View::new(&self.inner)
    }

    pub fn access(&mut self) -> borrows::Access<'_, T, Tag> {
        borrows::Access::new(&mut self.inner)
    }

    pub fn vacancy(&mut self) -> borrows::Vacancy<'_, T, Tag> {
        borrows::Vacancy::new(&mut self.inner)
    }
}
