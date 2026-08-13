use std::io;

use o3::collections::fixed::hash;

pub(in crate::backend::kqueue) struct Map<T>(hash::Map<(usize, T)>);

impl<T> Map<T> {
    /// # Safety
    /// The key must exist or this map must have spare capacity.
    pub(in crate::backend::kqueue::engine) unsafe fn upsert_unchecked(
        &mut self,
        key: usize,
        value: T,
        update: impl FnOnce(&mut T),
    ) {
        let hash = Self::hash(key);
        match unsafe { hash::raw::Map::entry_unchecked(&mut self.0, hash, |entry| entry.0 == key) }
        {
            hash::Entry::Occupied(mut entry) => update(&mut entry.get_mut().1),
            hash::Entry::Vacant(entry) => {
                entry.insert((key, value));
            }
        }
    }

    /// # Safety
    /// The key must exist.
    pub(in crate::backend::kqueue::engine) unsafe fn retain_unchecked(
        &mut self,
        key: usize,
        retain: impl FnOnce(&mut T) -> bool,
    ) {
        let hash = Self::hash(key);
        let mut entry = unsafe {
            hash::raw::Map::occupied_entry_unchecked(&mut self.0, hash, |entry| entry.0 == key)
        };
        if !retain(&mut entry.get_mut().1) {
            entry.remove();
        }
    }

    pub(in crate::backend::kqueue) fn try_with_capacity(capacity: usize) -> io::Result<Self> {
        use o3::collections::fixed::hash::{Map, Plan};
        let plan = Plan::new(capacity).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "kqueue hash table capacity exceeds the allocation layout",
            )
        })?;
        Ok(Self(Map::try_from_plan(plan)?))
    }

    fn hash(key: usize) -> u64 {
        let folded = key ^ (key >> 24);
        folded.wrapping_mul(0x9E37_79B9_7F4A_7C15usize) as u64
    }

    pub(in crate::backend::kqueue) fn try_insert(&mut self, key: usize, value: T) -> bool {
        self.0
            .try_insert(Self::hash(key), (key, value), |entry| entry.0 == key)
            .is_ok()
    }

    pub(in crate::backend::kqueue) fn get(&self, key: &usize) -> Option<&T> {
        self.0
            .get(Self::hash(*key), |entry| entry.0 == *key)
            .map(|entry| &entry.1)
    }

    pub(in crate::backend::kqueue) fn get_mut(&mut self, key: &usize) -> Option<&mut T> {
        self.0
            .get_mut(Self::hash(*key), |entry| entry.0 == *key)
            .map(|entry| &mut entry.1)
    }

    pub(in crate::backend::kqueue) fn contains_key(&self, key: &usize) -> bool {
        self.get(key).is_some()
    }

    pub(in crate::backend::kqueue) fn remove(&mut self, key: &usize) -> Option<T> {
        self.0
            .remove(Self::hash(*key), |entry| entry.0 == *key)
            .map(|entry| entry.1)
    }

    pub(in crate::backend::kqueue) fn values(&self) -> impl Iterator<Item = &T> {
        self.0.values().map(|entry| &entry.1)
    }

    pub(in crate::backend::kqueue) fn clear(&mut self) {
        self.0.clear();
    }
}
