use std::cell;

use dope_sys::table::permit;
use o3::{alloc, depot, depot::queue::indexed::fifo::staged};
pub(crate) use support::Support;

mod support;

/// Central generation-exact FIFO permit state.
///
/// The key lifetime is invariant and cannot be widened independently of the
/// storage that owns it.
#[repr(transparent)]
pub struct Fifo<Key: permit::raw::Key, I: depot::raw::Compact> {
    slots: cell::UnsafeCell<staged::Table<Key, (), (), I>>,
}

impl<Key: permit::raw::Key, I: depot::raw::Compact> Fifo<Key, I> {
    fn with_mut<T>(&self, access: impl FnOnce(&mut staged::Table<Key, (), (), I>) -> T) -> T {
        // SAFETY: Key and Compact forbid reentry during identity conversion;
        // the exclusive borrow cannot escape this operation.
        unsafe { access(&mut *self.slots.get()) }
    }

    fn take_inner(&self, index: I, key: Key, cancel: bool) -> permit::State<Key> {
        use permit::State;

        let identity = key.identity();
        self.with_mut(|slots| match slots.entry(index) {
            Some(staged::Entry::Vacant(_)) => State::Gone,
            Some(staged::Entry::Queued(entry)) => {
                if entry.key().identity() != identity {
                    return State::Stale;
                }
                if cancel {
                    entry.clear();
                }
                State::Waiting
            }
            Some(staged::Entry::Parked(entry)) => {
                if entry.key().identity() != identity {
                    return State::Stale;
                }
                State::Granted(entry.take().0)
            }
            None => State::Stale,
        })
    }
}

impl<Key: permit::raw::Key, I: depot::raw::Compact> permit::Queue for Fifo<Key, I> {
    type Key = Key;
    type Index = I;
    type Capability = Support;

    fn try_acquire() -> Result<Self::Capability, permit::Absent> {
        Ok(Support::Available)
    }

    fn try_arm(
        &self,
        &Support::Available: &Self::Capability,
        index: I,
        key: Key,
    ) -> Result<(), Key> {
        self.with_mut(|slots| {
            slots.vacant(index).ok_or(key)?.insert(key, ());
            Ok(())
        })
    }

    fn cancel(
        &self,
        &Support::Available: &Self::Capability,
        index: I,
        key: Key,
    ) -> permit::State<Key> {
        self.take_inner(index, key, true)
    }

    fn take(
        &self,
        &Support::Available: &Self::Capability,
        index: I,
        key: Key,
    ) -> permit::State<Key> {
        self.take_inner(index, key, false)
    }

    fn grant_one(&self, &Support::Available: &Self::Capability) -> Option<(I, Key)> {
        self.with_mut(|slots| {
            let entry = slots.front()?;
            let index = entry.index();
            let key = *entry.key();
            entry.set_parked(());
            Some((index, key))
        })
    }
}

impl<Key: permit::raw::Key, I: depot::raw::Compact> permit::Create for Fifo<Key, I> {
    fn try_new(capacity: usize) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: cell::UnsafeCell::new(staged::Table::try_new(capacity)?),
        })
    }
}
