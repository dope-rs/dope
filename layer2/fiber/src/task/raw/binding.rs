use core::{pin, task::Poll};

use crate::{
    context::wake,
    core::driver::schedule::ready::task::{self, pool, topology},
};

#[pin_project::pin_project(PinnedDrop)]
#[repr(transparent)]
pub(crate) struct Binding<'d, I: topology::raw::Index> {
    #[pin]
    node: task::raw::Node<'d, I>,
}

impl<'d, I: topology::raw::Index> Binding<'d, I> {
    pub(crate) const fn new() -> Self {
        use crate::core::driver::schedule::ready::task::raw::Node;

        Self { node: Node::new() }
    }

    /// Polls with a binding tied to this exact domain and reclaims on success.
    pub(crate) unsafe fn poll_domain<'a, Tag, T, const N: usize>(
        binding: pin::Pin<&'a Self>,
        edge: topology::Edge<'a, 'd, I>,
        domain: &mut task::Domain<'d, Tag, N>,
        poll: impl FnOnce(wake::Waker<'d>) -> Poll<T>,
    ) -> Result<Poll<(T, task::Reclaim)>, task::Error>
    where
        'd: 'a,
    {
        let node = binding.project_ref().node;
        let bound = match task::raw::Binding::get(node) {
            Some(bound) => bound,
            None => {
                let admit = domain.try_admit()?;
                unsafe { task::raw::Binding::bind_leased(node, admit, edge) }
            }
        };
        let wake = wake::Waker::from_parent(bound.waker());
        let Poll::Ready(output) = poll(wake) else {
            return Ok(Poll::Pending);
        };
        let claim = unsafe { bound.reclaim(domain) };
        Ok(Poll::Ready((output, claim)))
    }

    /// Polls with a binding tied to this exact pool and reclaims on success.
    pub(crate) unsafe fn poll_pool<'a, Tag, T, const N: usize>(
        binding: pin::Pin<&'a Self>,
        edge: topology::Edge<'a, 'd, I>,
        pool: &pool::Pool<'d, Tag, N>,
        root: pool::Root<'d>,
        poll: impl FnOnce(wake::Waker<'d>) -> Poll<T>,
    ) -> Result<Poll<T>, task::Error>
    where
        'd: 'a,
    {
        let node = binding.project_ref().node;
        let bound = match task::raw::Binding::get(node) {
            Some(bound) => bound,
            None => {
                let admit = pool.try_admit(root)?;
                unsafe { task::raw::Binding::bind_leased(node, admit, edge) }
            }
        };
        let wake = wake::Waker::from_parent(bound.waker());
        let Poll::Ready(output) = poll(wake) else {
            return Ok(Poll::Pending);
        };
        unsafe { bound.reclaim_pool(pool, root) };
        Ok(Poll::Ready(output))
    }

    pub(crate) fn reclaim<Tag, const N: usize>(
        binding: pin::Pin<&Self>,
        domain: &mut task::Domain<'d, Tag, N>,
    ) -> Option<task::Reclaim> {
        let node = binding.project_ref().node;
        let bound = task::raw::Binding::get(node)?;
        Some(unsafe { bound.reclaim(domain) })
    }

    pub(crate) fn reclaim_pool<Tag, const N: usize>(
        binding: pin::Pin<&Self>,
        pool: &pool::Pool<'d, Tag, N>,
        root: pool::Root<'d>,
    ) {
        let node = binding.project_ref().node;
        if let Some(bound) = task::raw::Binding::get(node) {
            unsafe { bound.reclaim_pool(pool, root) };
        }
    }
}

#[pin_project::pinned_drop]
impl<I: topology::raw::Index> PinnedDrop for Binding<'_, I> {
    fn drop(self: pin::Pin<&mut Self>) {
        task::raw::Binding::unbind(self.as_ref().project_ref().node);
    }
}
