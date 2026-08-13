use std::{io, marker, mem, process};

use o3::{buffer::resident, cell, queue};

use crate::{
    backend,
    driver::{settings, storage::ownership::state},
    platform,
};

type Buffer = <backend::Backend as platform::Buffer>::Token;

const NONE: u16 = u16::MAX;

type Invariant<'d> = marker::PhantomData<fn(&'d ()) -> &'d ()>;

pub(in crate::driver) struct Returned(queue::Fifo<Buffer>);

impl Returned {
    pub(in crate::driver) fn try_new(receive: settings::Receive) -> io::Result<Self> {
        Ok(Self(queue::Fifo::try_with_capacity(usize::from(
            receive.entries(),
        ))?))
    }

    pub(in crate::driver) fn push(&self, buffer: Buffer) {
        // SAFETY: capacity is the backend's buffer count. Buffer tokens are
        // linear and cannot occur in two ownership locations simultaneously.
        unsafe { queue::raw::Fifo::push_back_unchecked(&self.0, buffer) };
    }

    pub(in crate::driver) fn pop(&self) -> Option<Buffer> {
        self.0.pop_front()
    }

    pub(in crate::driver) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

struct Entry {
    refs: cell::LocalRefCount,
    state: state::State<Buffer>,
}

struct Binding<'d> {
    entry: &'d Entry,
    _brand: Invariant<'d>,
}

#[must_use]
pub(crate) struct RecvOwner<'d>(Binding<'d>);

#[must_use]
pub(crate) struct AccountedRecvOwner<'d>(Binding<'d>);

pub(in crate::driver) struct Owners {
    entries: Box<[Entry]>,
    counters: state::Counters,
    retained_limit: u16,
}

const _: () = {
    assert!(mem::size_of::<RecvOwner<'static>>() == mem::size_of::<usize>());
    assert!(mem::size_of::<AccountedRecvOwner<'static>>() == mem::size_of::<usize>());
};

impl<'d> Binding<'d> {
    fn new(entry: &'d Entry) -> Self {
        Self {
            entry,
            _brand: marker::PhantomData,
        }
    }
}

impl<'d> RecvOwner<'d> {
    fn new(entry: &'d Entry) -> Self {
        Self(Binding::new(entry))
    }

    fn entry(&self) -> &'d Entry {
        self.0.entry
    }
}

impl<'d> AccountedRecvOwner<'d> {
    fn new(entry: &'d Entry) -> Self {
        Self(Binding::new(entry))
    }

    fn entry(&self) -> &'d Entry {
        self.0.entry
    }
}

impl Owners {
    pub(in crate::driver) fn try_new(receive: settings::Receive) -> io::Result<Self> {
        use o3::collections::BoxSliceExt;

        let capacity = receive.entries();
        let entries = BoxSliceExt::try_box_with(usize::from(capacity), |index| {
            debug_assert!(index < usize::from(capacity));
            let index = index as u16;
            let successor = index.wrapping_add(1);
            let next = if successor < capacity {
                successor
            } else {
                NONE
            };
            Entry {
                refs: cell::LocalRefCount::empty(),
                state: state::State::new(index, next),
            }
        })
        .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self {
            entries,
            counters: state::Counters::new(),
            retained_limit: capacity - 1,
        })
    }

    fn acquire_entry(&self, buffer: Buffer) -> &Entry {
        let index = self.counters.free.get();
        let Some(entry) = self.entries.get(usize::from(index)) else {
            process::abort();
        };
        if !entry.refs.is_empty()
            || entry.state.retained.get() != 0
            || unsafe { (*entry.state.charge.get()).is_some() }
        {
            process::abort();
        }
        self.counters.free.set(entry.state.next.get());
        unsafe { (*entry.state.buffer.get()).write(buffer) };
        entry.refs.activate();
        entry
    }

    pub(in crate::driver) fn acquire(&self, buffer: Buffer) -> RecvOwner<'_> {
        RecvOwner::new(self.acquire_entry(buffer))
    }

    pub(in crate::driver) fn retain<'d>(&'d self, owner: &RecvOwner<'d>) -> RecvOwner<'d> {
        let entry = owner.entry();
        Self::retain_entry(entry);
        RecvOwner::new(entry)
    }

    pub(in crate::driver) fn acquire_accounted(
        &self,
        buffer: Buffer,
        budget: &resident::Budget<'_>,
        bytes: usize,
    ) -> Result<AccountedRecvOwner<'_>, Buffer> {
        if self.counters.retained.get() >= self.retained_limit {
            return Err(buffer);
        }
        let Ok(charge) = budget.try_charge(bytes) else {
            return Err(buffer);
        };
        let entry = self.acquire_entry(buffer);
        unsafe { *entry.state.charge.get() = Some(charge) };
        entry.state.retained.set(1);
        self.counters.retained.set(self.counters.retained.get() + 1);
        Ok(AccountedRecvOwner::new(entry))
    }

    fn retain_entry(entry: &Entry) {
        entry.refs.retain();
    }

    pub(in crate::driver) fn retain_accounted<'d>(
        &'d self,
        owner: &RecvOwner<'d>,
        budget: &resident::Budget<'_>,
        bytes: usize,
    ) -> Option<AccountedRecvOwner<'d>> {
        let entry = owner.entry();
        let retained = entry.state.retained.get();
        if retained == 0 {
            if self.counters.retained.get() >= self.retained_limit {
                return None;
            }
            if unsafe { (*entry.state.charge.get()).is_some() } {
                process::abort();
            }
            let Ok(charge) = budget.try_charge(bytes) else {
                return None;
            };
            unsafe { *entry.state.charge.get() = Some(charge) };
            self.counters.retained.set(self.counters.retained.get() + 1);
        }
        let Some(retained) = retained.checked_add(1) else {
            process::abort();
        };
        Self::retain_entry(entry);
        entry.state.retained.set(retained);
        Some(AccountedRecvOwner::new(entry))
    }

    pub(in crate::driver) fn retain_existing_accounted<'d>(
        &'d self,
        owner: &AccountedRecvOwner<'d>,
    ) -> AccountedRecvOwner<'d> {
        let entry = owner.entry();
        let retained = entry.state.retained.get();
        let Some(retained) = retained.checked_add(1) else {
            process::abort();
        };
        if retained == 1 || unsafe { (*entry.state.charge.get()).is_none() } {
            process::abort();
        }
        Self::retain_entry(entry);
        entry.state.retained.set(retained);
        AccountedRecvOwner::new(entry)
    }

    pub(in crate::driver) fn release_accounted(
        &self,
        owner: &AccountedRecvOwner<'_>,
    ) -> Option<Buffer> {
        let entry = owner.entry();
        let retained = entry.state.retained.get();
        if retained == 0 {
            process::abort();
        }
        entry.state.retained.set(retained - 1);
        if retained == 1 {
            drop(unsafe { (*entry.state.charge.get()).take() });
            let owners = self.counters.retained.get();
            if owners == 0 {
                process::abort();
            }
            self.counters.retained.set(owners - 1);
        }
        self.release_entry(entry)
    }

    pub(in crate::driver) fn release(&self, owner: &RecvOwner<'_>) -> Option<Buffer> {
        self.release_entry(owner.entry())
    }

    fn release_entry(&self, entry: &Entry) -> Option<Buffer> {
        if !entry.refs.release() {
            return None;
        }
        if entry.state.retained.get() != 0 || unsafe { (*entry.state.charge.get()).is_some() } {
            process::abort();
        }
        entry.refs.deactivate();
        entry.state.next.set(self.counters.free.get());
        self.counters.free.set(entry.state.index);
        Some(unsafe { (*entry.state.buffer.get()).assume_init_read() })
    }
}

impl Drop for Owners {
    fn drop(&mut self) {
        if self.counters.retained.get() != 0
            || self.entries.iter().any(|entry| {
                !entry.refs.is_empty()
                    || entry.state.retained.get() != 0
                    || unsafe { (*entry.state.charge.get()).is_some() }
            })
        {
            process::abort();
        }
    }
}
