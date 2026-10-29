//! Exact-owner runtime host for one manifold.

use std::pin;

use dope_runtime::topology::lending;

mod raw;

/// Layout-identical runtime owner for one installed manifold.
/// Pin projection preserves the executor's exact owner lifetime.
#[repr(transparent)]
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = crate, runtime = ::dope_runtime)]
pub struct Host<M> {
    #[pin]
    #[manifold]
    manifold: M,
}

impl<M> Host<M> {
    pub const fn new(manifold: M) -> Self {
        Self { manifold }
    }

    /// Borrows the exact pinned manifold owned by this host.
    pub fn manifold(self: pin::Pin<&Self>) -> pin::Pin<&M> {
        self.project_ref().manifold
    }
}

impl<'d, M> lending::Root<'d> for Host<M>
where
    M: lending::Root<'d>,
{
    type Provider = M::Provider;

    fn provider(self: pin::Pin<&Self>) -> pin::Pin<&Self::Provider> {
        let manifold = self.project_ref().manifold;
        lending::Root::provider(manifold)
    }
}

const _: () = {
    assert!(size_of::<Host<usize>>() == size_of::<usize>());
    assert!(align_of::<Host<usize>>() == align_of::<usize>());
};
