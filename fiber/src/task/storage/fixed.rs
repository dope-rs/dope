use std::{marker, pin, task};

use o3::collections::slab::pinned;

use crate::{abi, context, task::storage};

#[pin_project::pin_project(!Unpin)]
pub struct Slab<'d, F, const N: usize, Tag = ()>
where
    F: abi::Fiber<'d>,
{
    #[pin]
    inner: pinned::Fixed<F, N, Tag>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

#[must_use]
pub struct VacantEntry<'a, 'd, F, const N: usize, Tag = ()> {
    inner: pinned::FixedVacantEntry<'a, F, N, Tag>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F, const N: usize, Tag> VacantEntry<'_, 'd, F, N, Tag> {
    pub fn insert(self, fiber: F) -> storage::Id<'d, Tag> {
        storage::Id::from_key(self.inner.insert(fiber))
    }
}

impl<'d, F, const N: usize, Tag> Slab<'d, F, N, Tag>
where
    F: abi::Fiber<'d>,
{
    pub fn new() -> Self {
        use o3::collections::slab::pinned::Fixed;
        const {
            assert!(N > 0, "fiber slab capacity must be > 0");
        }
        Self {
            inner: Fixed::new(),
            driver: marker::PhantomData,
        }
    }

    pub fn insert(self: pin::Pin<&mut Self>, fiber: F) -> Option<storage::Id<'d, Tag>> {
        self.vacant_entry().map(|entry| entry.insert(fiber))
    }

    pub fn vacant_entry(self: pin::Pin<&mut Self>) -> Option<VacantEntry<'_, 'd, F, N, Tag>> {
        Some(VacantEntry {
            inner: self.project().inner.vacant_entry()?,
            driver: marker::PhantomData,
        })
    }

    /// Polls the exact generational member under the caller's shared turn budget.
    pub fn poll(
        self: pin::Pin<&mut Self>,
        id: &storage::Id<'d, Tag>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Option<task::Poll<F::Output>> {
        let fiber = self.project().inner.parts_mut(id.parts())?;
        Some(
            context
                .as_mut()
                .try_poll(fiber)
                .unwrap_or(task::Poll::Pending),
        )
    }

    pub fn remove(self: pin::Pin<&mut Self>, id: storage::Id<'d, Tag>) -> bool {
        self.project().inner.remove_parts(id.parts())
    }
}

impl<'d, F, const N: usize, Tag> Default for Slab<'d, F, N, Tag>
where
    F: abi::Fiber<'d>,
{
    fn default() -> Self {
        Self::new()
    }
}
