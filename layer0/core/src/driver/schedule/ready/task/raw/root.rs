use std::mem;

use o3::mem::Lease;

use crate::driver::schedule::{
    ready,
    ready::{task, task::topology},
};

/// Complete root binding that has no unbound representation.
pub struct Root<'d> {
    state: Lease<task::raw::State<'d>>,
}

impl<'d> Root<'d> {
    /// # Safety
    /// The admission must come from the root parent supplied to this entry.
    pub unsafe fn bind<I: topology::raw::Index>(
        admit: task::raw::Admission<'d>,
        edge: topology::Edge<'_, 'd, I>,
    ) -> Self {
        Self {
            state: Lease::new(unsafe { task::raw::State::bind(admit, edge) }, ()),
        }
    }

    pub fn parent(&self) -> ready::Target<'d> {
        ready::Target {
            address: self.state().parent.address(),
        }
    }

    pub fn poll(&self) -> (ready::Target<'d>, task::Parent<'d>) {
        (self.parent(), self.state().waker())
    }

    fn state(&self) -> &task::raw::State<'d> {
        self.state.sink()
    }
}

const _: () = {
    assert!(mem::size_of::<Root<'static>>() == mem::size_of::<task::raw::State<'static>>());
    assert!(mem::align_of::<Root<'static>>() == mem::align_of::<task::raw::State<'static>>());
};
