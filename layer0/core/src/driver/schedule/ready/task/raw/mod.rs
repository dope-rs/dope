mod binding;
mod root;

use std::{cell, marker};

pub use binding::Binding;
use o3::{
    base::marker::ThreadBound,
    mem,
    mem::{Return, lease::Open},
};
pub use root::Root;
use task::topology;

use crate::driver::schedule::{ready, ready::task};

#[repr(C)]
struct State<'d> {
    parent: task::Parent<'d>,
    child: ready::TaskKey<'d>,
}

impl<'d> State<'d> {
    unsafe fn bind<I: topology::raw::Index>(
        admit: Admission<'d>,
        edge: topology::Edge<'_, 'd, I>,
    ) -> Self {
        let (parent, held) = admit.into_parts();
        let (endpoint, local) = edge.into_parts();
        let child = parent.install_task(held, endpoint.key(), local.raw());
        Self { parent, child }
    }

    fn waker(&self) -> task::Parent<'d> {
        self.parent.retarget(self.child.key())
    }
}

impl Return for State<'_> {
    type Item = ();

    fn return_item(self, (): ()) {
        self.parent.release(self.child);
    }
}

#[must_use = "an admitted task must be bound or released"]
pub struct Admission<'d> {
    held: mem::Lease<Release<'d>, Open>,
}

struct Release<'d>(task::Parent<'d>);

impl<'d> Admission<'d> {
    pub(super) const fn new(parent: task::Parent<'d>, child: ready::Permit<'d>) -> Self {
        Self {
            held: mem::Lease::open(Release(parent), child),
        }
    }

    pub(super) fn into_parts(self) -> (task::Parent<'d>, ready::Permit<'d>) {
        let (Release(parent), child) = self.held.into_parts();
        (parent, child)
    }
}

impl<'d> Return for Release<'d> {
    type Item = ready::Permit<'d>;

    fn return_item(self, child: Self::Item) {
        self.0.release_hold(child);
    }
}

/// Exact domain admission whose unused credit returns through its lease.
#[must_use = "an admitted task must be bound or released"]
pub struct Lease<'lease, 'd> {
    held: mem::Lease<Recycle<'lease, 'd>, Open>,
}

struct Recycle<'lease, 'd> {
    parent: task::Parent<'d>,
    chain: &'lease ready::Chain,
}

impl<'lease, 'd> Lease<'lease, 'd> {
    pub(super) fn new(
        parent: task::Parent<'d>,
        child: ready::Permit<'d>,
        chain: &'lease ready::Chain,
    ) -> Self {
        Self {
            held: mem::Lease::open(Recycle { parent, chain }, child),
        }
    }

    pub(super) fn take(self) -> Admission<'d> {
        let (Recycle { parent, .. }, child) = self.held.into_parts();
        Admission::new(parent, child)
    }
}

impl<'d> Return for Recycle<'_, 'd> {
    type Item = ready::Permit<'d>;

    fn return_item(self, child: Self::Item) {
        self.parent.return_lease(child, self.chain);
    }
}

/// Pinned storage whose binding can only be removed as an affine guard.
#[pin_project::pin_project(!Unpin)]
pub struct Node<'d, I: topology::raw::Index> {
    #[pin]
    binding: cell::Cell<Option<State<'d>>>,
    local: marker::PhantomData<fn(I) -> I>,
    thread: ThreadBound,
}

impl<'d, I: topology::raw::Index> Node<'d, I> {
    pub const fn new() -> Self {
        Self {
            binding: cell::Cell::new(None),
            local: marker::PhantomData,
            thread: ThreadBound::NEW,
        }
    }
}

impl<I: topology::raw::Index> Default for Node<'_, I> {
    fn default() -> Self {
        Self::new()
    }
}

const _: () = {
    assert!(size_of::<Admission<'static>>() == 3 * size_of::<usize>());
    assert!(size_of::<Option<Admission<'static>>>() == size_of::<Admission<'static>>());
    assert!(size_of::<Lease<'static, 'static>>() == 4 * size_of::<usize>());
    assert!(size_of::<Option<Lease<'static, 'static>>>() == size_of::<Lease<'static, 'static>>());
};
