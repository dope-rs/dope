use std::{marker, mem, pin};

use task::lease;

use crate::driver::schedule::ready::{self, completion, task};

/// A linear, fixed-capacity set of task admissions beneath one exact parent.
///
/// The driver's free nodes remain shared, while this value linearly owns `N`
/// admission credits for `'d`. Its capacity is constant-size and independent
/// of `N`.
///
/// ```compile_fail
/// use dope_core::driver::schedule::ready::{completion, task::Domain};
///
/// fn widen<'d>(parent: completion::Wake<'d>) -> Domain<'static, (), 1> {
///     Domain::try_new(parent).unwrap()
/// }
/// ```
pub struct Domain<'d, Tag, const N: usize> {
    lease: lease::Credits<'d>,
    _tag: marker::PhantomData<fn(Tag) -> Tag>,
}

impl<'d, Tag, const N: usize> Domain<'d, Tag, N> {
    pub fn try_new(parent: completion::Wake<'d>) -> Result<Self, task::Error> {
        let lease = lease::Credits::try_new(parent, N)?;
        Ok(Self {
            lease,
            _tag: marker::PhantomData,
        })
    }

    #[must_use]
    pub fn admit<'lease, 'node>(
        &'lease mut self,
        node: pin::Pin<&'node task::Node<'d>>,
    ) -> Option<task::Lease<'lease, 'node, 'd>> {
        self.lease.admit(node)
    }

    pub(in crate::driver::schedule::ready::task) fn parent(&self) -> completion::Wake<'d> {
        self.lease.parent()
    }

    pub(in crate::driver::schedule::ready::task) fn reclaim(
        &mut self,
        reservation: ready::Reservation<'d>,
    ) {
        self.lease.reclaim(reservation);
    }

    pub fn retarget(&mut self, parent: completion::Wake<'d>) -> bool {
        self.lease.retarget(parent, N)
    }

    pub fn wake_parent(&self) {
        self.lease.wake_parent();
    }
}

const _: () =
    assert!(mem::size_of::<Domain<'static, (), 0>>() == mem::size_of::<lease::Credits<'static>>());
const _: () = assert!(
    mem::size_of::<Domain<'static, (), 4096>>() == mem::size_of::<Domain<'static, (), 0>>()
);
