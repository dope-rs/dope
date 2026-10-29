//! Driver-owned exact retained scheduling credits.

use crate::driver::route::indices;

pub mod error;
pub mod storage;

use std::mem::{align_of, size_of};

use o3::{mem, mem::lease};

use crate::{
    driver,
    driver::{route, schedule::ready},
};

/// Exact reason for scheduling the connection that owns a returned credit.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Wake {
    Returned = 1,
    Retry = 2,
}

/// Ready state observed while reclaiming one retained credit.
pub enum Take {
    Idle,
    Ready(Wake),
}

/// Exact ready key owned by retained-credit authority.
#[repr(transparent)]
pub(in crate::driver) struct Proof<'d> {
    key: ready::Stored,
    driver: driver::Invariant<'d>,
}

impl<'d> Proof<'d> {
    pub(in crate::driver) const fn new(key: ready::Stored, driver: driver::Invariant<'d>) -> Self {
        Self { key, driver }
    }

    pub(in crate::driver) const fn key(self) -> ready::Stored {
        self.key
    }
}

/// Route-branded identity of one receive-credit owner.
///
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Id<'d, ID: route::Tag> {
    target: route::Operation<'d, ID>,
}

impl<ID: route::Tag> PartialEq for Id<'_, ID> {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
    }
}

impl<ID: route::Tag> Eq for Id<'_, ID> {}

impl<'d, ID: route::Tag> Id<'d, ID> {
    /// Binds a route operation as a receive-credit identity.
    pub const fn new(target: route::Operation<'d, ID>) -> Self {
        Self { target }
    }

    /// Projects the bounded storage slot without erasing this operation identity.
    pub const fn slot(self) -> indices::SlotIndex {
        self.target.into_token().slot()
    }
}

/// An armed credit that cancels silently unless its consumer claims it.
#[must_use = "an armed credit must be claimed or cancelled"]
pub struct Pending<'d, ID: route::Tag> {
    lease: storage::Lease<'d, ready::Stored, Wake, ready::Arena>,
    driver: driver::Invariant<'d>,
    target: route::Operation<'d, ID>,
}

impl<'d, ID: route::Tag> Pending<'d, ID> {
    pub fn id(&self) -> Id<'d, ID> {
        Id::new(self.target)
    }

    pub fn claim(self) -> Held<'d, ID> {
        Held {
            lease: Return::new(self.lease, self.driver),
            target: self.target,
        }
    }

    pub fn cancel(self) {
        drop(self.lease);
    }
}

/// A claimed credit whose default terminal transition returns a resource.
#[must_use = "dropping a held credit returns its receive resource"]
pub struct Held<'d, ID: route::Tag> {
    lease: Return<'d>,
    target: route::Operation<'d, ID>,
}

impl<'d, ID: route::Tag> Held<'d, ID> {
    pub fn id(&self) -> Id<'d, ID> {
        Id::new(self.target)
    }

    pub fn cancel(self) {
        self.lease.cancel();
    }

    pub fn retry(self) {
        self.lease.retry();
    }

    pub fn into_return(self) -> Return<'d> {
        self.lease
    }
}

/// Generation-checked, non-owning access to one retained-credit slot.
/// Arming adds the exact route and driver-owned readiness target atomically.
#[derive(Clone, Copy)]
pub struct Control<'d> {
    key: storage::Key<'d, ready::Stored, Wake, ready::Arena>,
    driver: driver::Invariant<'d>,
}

/// Route-bound credit control awaiting one exact readiness owner.
#[derive(Clone, Copy)]
pub struct Binding<'d, ID: route::Tag> {
    control: Control<'d>,
    target: route::Operation<'d, ID>,
}

impl<'d> Control<'d> {
    pub const fn bind<ID: route::Tag>(self, target: route::Operation<'d, ID>) -> Binding<'d, ID> {
        Binding {
            control: self,
            target,
        }
    }

    pub fn arm<ID: route::Tag>(
        self,
        target: route::Operation<'d, ID>,
        ready: ready::Target<'d>,
    ) -> Result<Pending<'d, ID>, error::Arm> {
        let (_, ready) = ready.receive();
        let lease = self.key.arm(ready)?;
        Ok(Pending {
            lease,
            driver: self.driver,
            target,
        })
    }
}

impl<'d, ID: route::Tag> Binding<'d, ID> {
    pub fn into_parts(self) -> (Control<'d>, route::Operation<'d, ID>) {
        (self.control, self.target)
    }
}

/// Fixed retained-credit storage owned by one driver generation.
pub struct Storage<'d> {
    pub(in crate::driver) entries: storage::Slots<'d, ready::Stored, Wake, ready::Arena>,
    driver: driver::Invariant<'d>,
}

impl<'d> Storage<'d> {
    /// Checks the index once and borrows its exact active storage generation.
    pub fn get(&self, index: indices::SlotIndex) -> Result<Slot<'_, 'd>, error::Index> {
        Ok(Slot {
            entry: self.entries.get(index)?,
            driver: self.driver,
        })
    }
}

/// Borrowed active credit with infallible reads and driver-bound identity.
#[derive(Clone, Copy)]
pub struct Slot<'a, 'd> {
    entry: storage::Slot<'a, 'd, ready::Stored, Wake, ready::Arena>,
    driver: driver::Invariant<'d>,
}

impl<'d> Slot<'_, 'd> {
    /// Captures a checked generation for control that may outlive the storage.
    pub fn control(self) -> Control<'d> {
        Control {
            key: self.entry.key(),
            driver: self.driver,
        }
    }

    pub fn is_active(self) -> bool {
        self.entry.is_active()
    }

    /// Whether an outstanding lease still owns this slot's receive resource.
    pub fn is_held(self) -> bool {
        self.entry.is_held()
    }

    pub fn take(self) -> Take {
        match self.entry.take() {
            Some(wake) => Take::Ready(wake),
            None => Take::Idle,
        }
    }
}

const _: () = {
    assert!(size_of::<Proof<'static>>() == size_of::<ready::Stored>());
    assert!(align_of::<Proof<'static>>() == align_of::<ready::Stored>());
    assert!(size_of::<Wake>() == size_of::<u8>());
    assert!(size_of::<Take>() == size_of::<u8>());
    assert!(size_of::<error::Arm>() == 3 * size_of::<usize>());
    assert!(size_of::<Slot<'static, 'static>>() == size_of::<usize>());
    assert!(size_of::<route::Operation<'static, route::KeyTag<0>>>() == 2 * size_of::<usize>());
    assert!(size_of::<Control<'static>>() == 2 * size_of::<usize>());
    assert!(
        size_of::<Binding<'static, route::KeyTag<0>>>()
            == size_of::<Control<'static>>()
                + size_of::<route::Operation<'static, route::KeyTag<0>>>()
    );
    assert!(size_of::<Id<'static, route::KeyTag<0>>>() == 2 * size_of::<usize>());
    assert!(size_of::<Storage<'static>>() == 4 * size_of::<usize>());
    assert!(size_of::<Pending<'static, route::KeyTag<0>>>() == 4 * size_of::<usize>());
    assert!(
        size_of::<Held<'static, route::KeyTag<0>>>()
            == size_of::<Pending<'static, route::KeyTag<0>>>()
    );
    assert!(size_of::<Return<'static>>() == 2 * size_of::<usize>());
    assert!(size_of::<Option<Return<'static>>>() == size_of::<Return<'static>>());
};

/// Terminal receive credit retained by its returned resource.
#[must_use = "dropping a return credit schedules its resource owner"]
#[repr(transparent)]
pub struct Return<'d> {
    lease: mem::Lease<Notify<'d>, lease::Open>,
}

struct Notify<'d> {
    driver: driver::Invariant<'d>,
}

impl<'d> Return<'d> {
    fn new(
        lease: storage::Lease<'d, ready::Stored, Wake, ready::Arena>,
        driver: driver::Invariant<'d>,
    ) -> Self {
        Self {
            lease: mem::Lease::open(Notify { driver }, lease),
        }
    }

    fn cancel(self) {
        let (_, lease) = self.lease.into_parts();
        drop(lease);
    }

    fn retry(self) {
        let (notify, lease) = self.lease.into_parts();
        lease.release_with(Wake::Retry, |owner, target| {
            owner
                .stored()
                .credit_ready(Proof::new(target, notify.driver));
        });
    }
}

impl<'d> mem::Return for Notify<'d> {
    type Item = storage::Lease<'d, ready::Stored, Wake, ready::Arena>;

    fn return_item(self, lease: Self::Item) {
        lease.release_with(Wake::Returned, |owner, target| {
            owner.stored().credit_ready(Proof::new(target, self.driver));
        });
    }
}
