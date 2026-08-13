use std::{marker, mem, pin};

use crate::driver::schedule::ready::{self, completion, task};

/// Exact pinned task admitted beneath one live parent wake.
///
/// ```compile_fail
/// use dope_core::driver::schedule::ready::{completion, task};
/// use std::pin::Pin;
///
/// fn escape<'a, 'd>(
///     parent: completion::Wake<'d>,
///     node: Pin<&'a task::Node<'d>>,
/// ) -> task::Admission<'static, 'static, 'd> {
///     task::raw::Binding::admit(parent, node).unwrap()
/// }
/// ```
#[must_use = "an admitted task must be bound or released"]
pub struct Admission<'lease, 'node, 'd> {
    pub(in crate::driver::schedule::ready::task) node: pin::Pin<&'node task::Node<'d>>,
    pub(in crate::driver::schedule::ready::task) parent: completion::Wake<'d>,
    pub(in crate::driver::schedule::ready::task) child: ready::Reservation<'d>,
    _lease: marker::PhantomData<&'lease mut ()>,
}

impl<'lease, 'node, 'd> Admission<'lease, 'node, 'd> {
    pub(super) fn leased(
        node: pin::Pin<&'node task::Node<'d>>,
        parent: completion::Wake<'d>,
        child: ready::Reservation<'d>,
    ) -> Self {
        Self {
            node,
            parent,
            child,
            _lease: marker::PhantomData,
        }
    }

    pub(super) fn global(
        node: pin::Pin<&'node task::Node<'d>>,
        parent: completion::Wake<'d>,
        child: ready::Reservation<'d>,
    ) -> Admission<'node, 'node, 'd>
    where
        'd: 'node,
    {
        Admission {
            node,
            parent,
            child,
            _lease: marker::PhantomData,
        }
    }
}

impl Drop for Admission<'_, '_, '_> {
    fn drop(&mut self) {
        self.parent.0.release_admission(&self.child);
    }
}

const _: () =
    assert!(mem::size_of::<Admission<'static, 'static, 'static>>() == 4 * mem::size_of::<usize>());
