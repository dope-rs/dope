use std::pin;

use o3::mem::{Lease, Return, lease::Open};

use crate::driver::schedule::{
    ready,
    ready::{
        task,
        task::{pool, topology},
    },
};

/// Affine proof that one node currently owns an installed task binding.
#[must_use = "a binding must be reclaimed or released"]
pub struct Binding<'node, 'd, I: topology::raw::Index> {
    state: Lease<Restore<'node, 'd, I>, Open>,
}

struct Restore<'node, 'd, I: topology::raw::Index>(pin::Pin<&'node task::raw::Node<'d, I>>);

impl<'node, 'd, I: topology::raw::Index> Binding<'node, 'd, I> {
    pub fn get(node: pin::Pin<&'node task::raw::Node<'d, I>>) -> Option<Self> {
        let state = node.binding.take()?;
        Some(Self {
            state: Lease::open(Restore(node), state),
        })
    }

    /// # Safety
    /// The node must be vacant and remain pinned for the returned binding's
    /// lifetime. The admission and edge must belong to the same driver.
    pub unsafe fn bind_leased<'lease>(
        node: pin::Pin<&'node task::raw::Node<'d, I>>,
        admit: task::raw::Lease<'lease, 'd>,
        edge: topology::Edge<'node, 'd, I>,
    ) -> Self {
        Self {
            state: Lease::open(Restore(node), unsafe {
                task::raw::State::bind(admit.take(), edge)
            }),
        }
    }

    pub fn waker(&self) -> task::Parent<'d> {
        self.state().waker()
    }

    /// Revokes the generational wake slot and removes its topology membership.
    pub fn release(self) {
        self.into_state().return_item(());
    }

    /// # Safety
    /// This binding must have been admitted by this exact domain.
    pub unsafe fn reclaim<Tag, const N: usize>(
        self,
        domain: &mut task::Domain<'d, Tag, N>,
    ) -> task::Reclaim {
        let held = self.reclaim_task();
        domain.reclaim(held)
    }

    /// # Safety
    /// This binding must have been admitted by this exact pool and root.
    pub unsafe fn reclaim_pool<Tag, const N: usize>(
        self,
        pool: &pool::Pool<'d, Tag, N>,
        root: pool::Root<'d>,
    ) {
        let held = self.reclaim_task();
        pool.reclaim(root, held, (), ())
    }

    fn reclaim_task(self) -> ready::Reclaimed<'d> {
        let state = self.into_state();
        state.parent.reclaim_task(state.child)
    }

    fn into_state(self) -> task::raw::State<'d> {
        self.state.into_parts().1
    }

    fn state(&self) -> &task::raw::State<'d> {
        self.state.item()
    }

    pub fn unbind(node: pin::Pin<&'node task::raw::Node<'d, I>>) {
        if let Some(binding) = Self::get(node) {
            binding.release();
        }
    }
}

impl<'d, I: topology::raw::Index> Return for Restore<'_, 'd, I> {
    type Item = task::raw::State<'d>;

    fn return_item(self, state: Self::Item) {
        self.0.binding.set(Some(state));
    }
}
