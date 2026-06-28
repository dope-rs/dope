use crate::backend;

enum Entry<T> {
    Free {
        next: u32,
        epoch: backend::token::Epoch,
    },
    Occupied {
        value: T,
        epoch: backend::token::Epoch,
    },
}

impl<T> Entry<T> {
    fn epoch(&self) -> backend::token::Epoch {
        match self {
            Self::Free { epoch, .. } | Self::Occupied { epoch, .. } => *epoch,
        }
    }
}

const NO_FREE: u32 = u32::MAX;

// `place_at` and `reserve`/`alloc` must not mix on one Slab: the former bypasses the freelist.
pub(super) struct Slab<T> {
    slots: Vec<Entry<T>>,
    next_free: u32,
    len: usize,
    cap: usize,
}

impl<T> Slab<T> {
    #[must_use]
    pub(super) fn new(cap: usize) -> Self {
        Self {
            slots: Vec::new(),
            next_free: NO_FREE,
            len: 0,
            cap,
        }
    }

    /// Pre-allocates all `cap` slots up front so `slots` never reallocates and
    /// entry addresses stay fixed until removal — required when an entry is
    /// self-referential (an iovec/msghdr pointing into the entry itself).
    #[must_use]
    pub(super) fn pinned(cap: usize) -> Self {
        Self {
            slots: Vec::with_capacity(cap),
            ..Self::new(cap)
        }
    }

    pub(super) fn slot_count(&self) -> usize {
        self.cap
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push_free(&mut self, next: u32) {
        self.slots.push(Entry::Free {
            next,
            epoch: backend::token::Epoch::INITIAL,
        });
    }

    fn grow_through(&mut self, index: usize) {
        while self.slots.len() <= index {
            let here = self.slots.len() as u32;
            let next = if here as usize == index {
                NO_FREE
            } else {
                self.next_free
            };
            self.push_free(next);
            if here as usize != index {
                self.next_free = here;
            }
        }
    }

    fn pop_free(&mut self) -> Option<(usize, backend::token::Epoch)> {
        if self.next_free == NO_FREE {
            if self.slots.len() >= self.cap {
                return None;
            }
            let idx = self.slots.len();
            self.push_free(NO_FREE);
            return Some((idx, backend::token::Epoch::INITIAL));
        }
        let idx = self.next_free as usize;
        let Entry::Free { next, epoch } = self.slots[idx] else {
            panic!("slab invariant violated: freelist points to occupied slot");
        };
        self.next_free = next;
        Some((idx, epoch))
    }

    pub(super) fn alloc(&mut self, value: T) -> Option<backend::token::Key> {
        let (idx, epoch) = self.pop_free()?;
        self.slots[idx] = Entry::Occupied { value, epoch };
        self.len += 1;
        Some(backend::token::Key::new(
            backend::token::LocalIdx::new(idx as u32),
            epoch,
        ))
    }

    pub(super) fn place_at<F: FnOnce(backend::token::Key) -> T>(
        &mut self,
        index: backend::token::LocalIdx,
        make: F,
    ) -> backend::token::Key {
        let i = index.raw() as usize;
        assert!(i < self.cap, "slab::place_at index out of range");
        self.grow_through(i);
        let epoch = self.slots[i].epoch().bump();
        if matches!(self.slots[i], Entry::Free { .. }) {
            self.len += 1;
        }
        let key = backend::token::Key::new(index, epoch);
        self.slots[i] = Entry::Occupied {
            value: make(key),
            epoch,
        };
        key
    }

    pub(super) fn reserve(&mut self) -> Option<Reservation<'_, T>> {
        let (idx, epoch) = self.pop_free()?;
        let epoch = epoch.bump();
        Some(Reservation {
            slab: self,
            index: backend::token::LocalIdx::new(idx as u32),
            epoch,
        })
    }

    pub(super) fn get(&self, key: backend::token::Key) -> Option<&T> {
        match self.slots.get(key.index().raw() as usize)? {
            Entry::Occupied { value, epoch } if *epoch == key.epoch() => Some(value),
            _ => None,
        }
    }

    pub(super) fn get_mut(&mut self, key: backend::token::Key) -> Option<&mut T> {
        match self.slots.get_mut(key.index().raw() as usize)? {
            Entry::Occupied { value, epoch } if *epoch == key.epoch() => Some(value),
            _ => None,
        }
    }

    pub(super) fn remove(&mut self, key: backend::token::Key) -> bool {
        let i = key.index().raw() as usize;
        let Some(Entry::Occupied { epoch, .. }) = self.slots.get(i) else {
            return false;
        };
        if *epoch != key.epoch() {
            return false;
        }
        let next_epoch = epoch.bump();
        self.slots[i] = Entry::Free {
            next: self.next_free,
            epoch: next_epoch,
        };
        self.next_free = key.index().raw();
        self.len -= 1;
        true
    }

    pub(super) fn at_index(
        &self,
        index: backend::token::LocalIdx,
    ) -> Option<(&T, backend::token::Epoch)> {
        match self.slots.get(index.raw() as usize)? {
            Entry::Occupied { value, epoch } => Some((value, *epoch)),
            Entry::Free { .. } => None,
        }
    }

    pub(super) fn at_index_mut(&mut self, index: backend::token::LocalIdx) -> Option<&mut T> {
        match self.slots.get_mut(index.raw() as usize)? {
            Entry::Occupied { value, .. } => Some(value),
            Entry::Free { .. } => None,
        }
    }

    pub(super) fn epoch(&self, index: backend::token::LocalIdx) -> Option<backend::token::Epoch> {
        Some(self.slots.get(index.raw() as usize)?.epoch())
    }
}

pub(super) struct Reservation<'a, T> {
    slab: &'a mut Slab<T>,
    index: backend::token::LocalIdx,
    epoch: backend::token::Epoch,
}

impl<'a, T> Reservation<'a, T> {
    pub(super) fn index(&self) -> backend::token::LocalIdx {
        self.index
    }

    pub(super) fn epoch(&self) -> backend::token::Epoch {
        self.epoch
    }

    pub(super) fn fill(self, value: T) -> backend::token::Key {
        let i = self.index.raw() as usize;
        let epoch = self.epoch;
        self.slab.slots[i] = Entry::Occupied { value, epoch };
        self.slab.len += 1;
        let key = backend::token::Key::new(self.index, epoch);
        std::mem::forget(self);
        key
    }
}

impl<'a, T> Drop for Reservation<'a, T> {
    fn drop(&mut self) {
        self.slab.next_free = self.index.raw();
    }
}
