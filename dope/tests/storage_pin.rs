use std::cell::Cell;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;

use dope_test as common;

extern crate dope;
use dope::runtime::StorageFactory;

struct Factory;

struct Pinned {
    address: Cell<Option<NonNull<Self>>>,
    _pin: PhantomPinned,
}

impl Pinned {
    fn record(self: Pin<&Self>) {
        self.address.set(Some(NonNull::from(self.get_ref())));
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        assert_eq!(self.address.get(), Some(NonNull::from(&mut *self)));
    }
}

impl StorageFactory for Factory {
    type Output<'d> = Pinned;

    fn build<'d>(self, _driver: &mut dope::DriverContext<'_, 'd>) -> Self::Output<'d> {
        Pinned {
            address: Cell::new(None),
            _pin: PhantomPinned,
        }
    }
}

#[test]
fn storage_is_dropped_in_place_after_pinning() {
    let exec = common::exec().with_storage_factory(Factory);
    exec.enter(|session| session.storage_pin().record());
}
