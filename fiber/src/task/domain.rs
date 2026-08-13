use dope::core::driver::schedule::ready::task;

use crate::context;

/// A linear set of `N` driver-ready task slots owned for one root lifetime.
/// `Tag` distinguishes roles; credits use constant space and no reallocations.
#[repr(transparent)]
pub(crate) struct Domain<'d, Tag, const N: usize> {
    inner: task::Domain<'d, Tag, N>,
}

impl<'d, Tag, const N: usize> Domain<'d, Tag, N> {
    pub(crate) fn try_new(parent: context::RootWaker<'d>) -> Result<Self, super::Error> {
        let parent: context::Waker<'d> = parent.into();
        Ok(Self {
            inner: task::Domain::try_new(parent.0).map_err(super::Error::from)?,
        })
    }

    pub(crate) fn inner(&mut self) -> &mut task::Domain<'d, Tag, N> {
        &mut self.inner
    }

    pub(crate) fn wake_parent(&self) {
        self.inner.wake_parent();
    }

    pub(crate) fn retarget(&mut self, parent: context::RootWaker<'d>) -> bool {
        let parent: context::Waker<'d> = parent.into();
        self.inner.retarget(parent.0)
    }
}
