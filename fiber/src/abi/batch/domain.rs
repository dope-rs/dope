use crate::{abi::batch, context, task};

/// Ready-slot ownership dedicated to one fixed-capacity batch.
#[repr(transparent)]
pub struct Domain<'d, const N: usize> {
    inner: task::Domain<'d, batch::DomainTag, N>,
}

impl<'d, const N: usize> Domain<'d, N> {
    pub fn try_new(parent: context::RootWaker<'d>) -> Result<Self, task::Error> {
        Ok(Self {
            inner: task::Domain::try_new(parent)?,
        })
    }

    pub const fn acquire() -> task::AcquireBatch<N> {
        task::AcquireBatch::new()
    }

    pub(super) fn inner(&mut self) -> &mut task::Domain<'d, batch::DomainTag, N> {
        &mut self.inner
    }

    pub(super) fn retarget(&mut self, parent: context::RootWaker<'d>) -> bool {
        self.inner.retarget(parent)
    }
}
