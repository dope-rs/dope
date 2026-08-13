use std::{io, process, ptr};

use o3::{collections, collections::fixed::hash};

use crate::backend::kqueue;

#[derive(Clone, Copy, Eq, PartialEq)]
struct Key {
    ident: libc::uintptr_t,
    filter: i16,
}

impl Key {
    fn of(change: &libc::kevent) -> Self {
        Self {
            ident: change.ident,
            filter: change.filter,
        }
    }

    fn hash(self) -> u64 {
        let filter = self.filter as u16 as usize;
        let folded = self.ident ^ self.ident.rotate_right(23) ^ filter.rotate_left(11);
        folded.wrapping_mul(0x9E37_79B9_7F4A_7C15usize) as u64
    }
}

/// Fixed-capacity kqueue changes indexed by kernel registration identity.
pub(crate) struct Changes {
    raw: Vec<libc::kevent>,
    indices: hash::Map<(Key, usize)>,
    normal_limit: usize,
    wake_queued: bool,
}

impl Changes {
    pub(in crate::backend::kqueue) fn try_with_capacity(active: usize) -> io::Result<Self> {
        use o3::collections::fixed::hash::{Map, Plan};

        let total = active.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "kqueue change capacity overflows the platform",
            )
        })?;
        let raw = collections::VecExt::try_vec_with_capacity(total)?;
        let plan = Plan::new(total).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "kqueue change capacity exceeds the allocation layout",
            )
        })?;
        let indices = Map::try_from_plan(plan)?;
        Ok(Self {
            raw,
            indices,
            normal_limit: active,
            wake_queued: false,
        })
    }

    pub(in crate::backend::kqueue) fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    pub(in crate::backend::kqueue) fn len(&self) -> usize {
        self.raw.len()
    }

    pub(in crate::backend::kqueue) fn as_slice(&self) -> &[libc::kevent] {
        &self.raw
    }

    pub(in crate::backend::kqueue) fn as_mut_slice(&mut self) -> &mut [libc::kevent] {
        &mut self.raw
    }

    pub(in crate::backend::kqueue) fn tail(&self, limit: usize) -> &[libc::kevent] {
        let count = self.raw.len().min(limit);
        &self.raw[self.raw.len() - count..]
    }

    pub(in crate::backend::kqueue) fn commit_tail(&mut self, count: usize) {
        let split = self.raw.len() - count;
        for change in &self.raw[split..] {
            let key = Key::of(change);
            let removed = self.indices.remove(key.hash(), |entry| entry.0 == key);
            if removed.is_none() {
                process::abort();
            }
            if key == Self::wake_key() {
                self.wake_queued = false;
            }
        }
        self.raw.truncate(split);
    }

    pub(in crate::backend::kqueue) fn try_upsert(&mut self, change: libc::kevent) -> bool {
        if Key::of(&change) == Self::wake_key() {
            return false;
        }
        let limit = self.normal_limit + usize::from(self.wake_queued);
        self.upsert(change, limit)
    }

    pub(in crate::backend::kqueue) fn wake(&mut self) {
        let change = libc::kevent {
            ident: kqueue::WAKE_IDENT,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ENABLE,
            fflags: libc::NOTE_TRIGGER,
            data: 0,
            udata: ptr::null_mut(),
        };
        let total = self.normal_limit + 1;
        if !self.upsert(change, total) {
            process::abort();
        }
        self.wake_queued = true;
    }

    fn upsert(&mut self, change: libc::kevent, limit: usize) -> bool {
        let key = Key::of(&change);
        if let Some(index) = self.index(key) {
            let current = &mut self.raw[index];
            if current.flags & libc::EV_ADD != 0
                && change.flags & (libc::EV_ADD | libc::EV_DELETE) == 0
            {
                current.flags |= change.flags;
                current.fflags |= change.fflags;
                current.udata = change.udata;
            } else {
                *current = change;
            }
            return true;
        }
        if self.raw.len() >= limit {
            return false;
        }
        let index = self.raw.len();
        if self
            .indices
            .try_insert(key.hash(), (key, index), |entry| entry.0 == key)
            .is_err()
        {
            return false;
        }
        self.raw.push(change);
        true
    }

    pub(in crate::backend::kqueue) fn remove(
        &mut self,
        ident: libc::uintptr_t,
        filter: i16,
    ) -> bool {
        let key = Key { ident, filter };
        let Some((_, index)) = self.indices.remove(key.hash(), |entry| entry.0 == key) else {
            return false;
        };
        if key == Self::wake_key() {
            self.wake_queued = false;
        }
        let last = self.raw.len() - 1;
        self.raw.swap_remove(index);
        if index != last {
            let moved = Key::of(&self.raw[index]);
            let Some(slot) = self.indices.get_mut(moved.hash(), |entry| entry.0 == moved) else {
                process::abort();
            };
            slot.1 = index;
        }
        true
    }

    pub(in crate::backend::kqueue) fn clear(&mut self) {
        self.raw.clear();
        self.indices.clear();
        self.wake_queued = false;
    }

    fn index(&self, key: Key) -> Option<usize> {
        self.indices
            .get(key.hash(), |entry| entry.0 == key)
            .map(|entry| entry.1)
    }

    const fn wake_key() -> Key {
        Key {
            ident: kqueue::WAKE_IDENT,
            filter: libc::EVFILT_USER,
        }
    }
}
