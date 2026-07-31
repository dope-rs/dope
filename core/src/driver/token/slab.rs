use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;

use o3::collections::{CellSlab, Slab, SlabGeneration, SlabKey, SlabKeyParts, SlabVacantEntry};

use super::{EPOCH_MASK, SLOT_MASK, SlotIndex, Token, TokenTag};

const GENERATION_LIMIT: u32 = EPOCH_MASK as u32;

#[repr(transparent)]
pub struct Key<Tag>(SlabKey<Tag, GENERATION_LIMIT>);

#[repr(transparent)]
pub struct KeyParts<Tag> {
    parts: SlabKeyParts<GENERATION_LIMIT>,
    tag: PhantomData<*mut Tag>,
}

#[repr(transparent)]
pub struct TokenCellSlab<T, Tag>(CellSlab<T, Tag, GENERATION_LIMIT>);

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct TokenCapacity(usize);

#[repr(transparent)]
pub struct TokenSlab<T, Tag>(Slab<T, Tag, GENERATION_LIMIT>);

#[repr(transparent)]
pub struct TokenSlabVacantEntry<'a, T, Tag>(SlabVacantEntry<'a, T, Tag, GENERATION_LIMIT>);

impl<Tag> Clone for Key<Tag> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Tag> Copy for Key<Tag> {}

impl<Tag> PartialEq for Key<Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<Tag> Eq for Key<Tag> {}

impl<Tag> Hash for Key<Tag> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<Tag> fmt::Debug for Key<Tag> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<Tag> Key<Tag> {
    const fn from_raw(raw: SlabKey<Tag, GENERATION_LIMIT>) -> Self {
        Self(raw)
    }

    const fn raw(self) -> SlabKey<Tag, GENERATION_LIMIT> {
        self.0
    }

    pub const fn index(self) -> u32 {
        self.0.index()
    }

    pub const fn slot(self) -> SlotIndex {
        SlotIndex::from_bounded(self.0.index())
    }

    pub const fn generation(self) -> SlabGeneration<GENERATION_LIMIT> {
        self.0.generation()
    }
}

impl<Tag> Clone for KeyParts<Tag> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Tag> Copy for KeyParts<Tag> {}

impl<Tag> KeyParts<Tag> {
    pub(super) const fn new(index: u32, generation: u32) -> Option<Self> {
        match SlabKeyParts::new(index, generation) {
            Some(parts) => Some(Self::from_raw(parts)),
            None => None,
        }
    }

    const fn from_raw(parts: SlabKeyParts<GENERATION_LIMIT>) -> Self {
        Self {
            parts,
            tag: PhantomData,
        }
    }

    const fn raw(self) -> SlabKeyParts<GENERATION_LIMIT> {
        self.parts
    }

    pub const fn index(self) -> u32 {
        self.parts.index()
    }

    pub const fn slot(self) -> SlotIndex {
        SlotIndex::from_bounded(self.parts.index())
    }

    pub(super) const fn generation(self) -> SlabGeneration<GENERATION_LIMIT> {
        self.parts.generation()
    }
}

impl TokenCapacity {
    pub const EMPTY: Self = Self(0);

    pub const fn new(capacity: usize) -> Option<Self> {
        if capacity <= SLOT_MASK as usize + 1 {
            Some(Self(capacity))
        } else {
            None
        }
    }

    pub const fn get(self) -> usize {
        self.0
    }

    pub const fn slot(self, index: usize) -> Option<SlotIndex> {
        if index < self.0 {
            Some(SlotIndex::from_bounded(index as u32))
        } else {
            None
        }
    }

    pub const fn sentinel(self) -> Option<SlotIndex> {
        SlotIndex::try_new(self.0 as u32)
    }

    pub fn slots(self) -> impl ExactSizeIterator<Item = SlotIndex> + DoubleEndedIterator {
        (0..self.0 as u32).map(SlotIndex::from_bounded)
    }
}

impl<T, Tag> TokenSlab<T, Tag> {
    #[must_use]
    pub fn with_capacity(capacity: TokenCapacity) -> Self {
        Self(Slab::with_capacity(capacity.get()))
    }

    pub fn capacity(&self) -> TokenCapacity {
        TokenCapacity(self.0.capacity())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn insert(&mut self, value: T) -> Result<Key<Tag>, T> {
        self.0.insert(value).map(Key::from_raw)
    }

    pub fn insert_entry(&mut self, value: T) -> Result<(Key<Tag>, &mut T), T> {
        self.0
            .insert_entry(value)
            .map(|(key, value)| (Key::from_raw(key), value))
    }

    pub fn vacant_entry(&mut self) -> Option<TokenSlabVacantEntry<'_, T, Tag>> {
        self.0.vacant_entry().map(TokenSlabVacantEntry)
    }

    pub fn vacant_entry_at(&mut self, index: u32) -> Option<TokenSlabVacantEntry<'_, T, Tag>> {
        self.0.vacant_entry_at(index).map(TokenSlabVacantEntry)
    }

    pub fn get(&self, key: Key<Tag>) -> Option<&T> {
        self.0.get(key.raw())
    }

    pub fn get_parts(&self, parts: KeyParts<Tag>) -> Option<&T> {
        self.0.get_parts(parts.raw())
    }

    pub fn get_parts_mut(&mut self, parts: KeyParts<Tag>) -> Option<&mut T> {
        self.0.get_parts_mut(parts.raw())
    }

    pub fn remove(&mut self, key: Key<Tag>) -> Option<T> {
        self.0.remove(key.raw())
    }

    pub fn remove_parts(&mut self, parts: KeyParts<Tag>) -> Option<T> {
        self.0.remove_parts(parts.raw())
    }

    pub fn remove_index_with<R>(
        &mut self,
        index: u32,
        f: impl FnOnce(&mut T, Key<Tag>) -> Option<R>,
    ) -> Option<(T, R)> {
        self.0
            .remove_index_with(index, |value, key| f(value, Key::from_raw(key)))
    }

    pub fn get_index(&self, index: u32) -> Option<(&T, Key<Tag>)> {
        self.0
            .get_index(index)
            .map(|(value, key)| (value, Key::from_raw(key)))
    }

    pub fn get_index_mut(&mut self, index: u32) -> Option<(&mut T, Key<Tag>)> {
        self.0
            .get_index_mut(index)
            .map(|(value, key)| (value, Key::from_raw(key)))
    }

    pub fn key(&self, index: u32) -> Option<Key<Tag>> {
        self.0.key(index).map(Key::from_raw)
    }

    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.0.values()
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.0.values_mut()
    }
}

impl<T, Tag> TokenSlabVacantEntry<'_, T, Tag> {
    pub fn insert(self, value: T) {
        let _ = self.0.insert(value);
    }
}

impl<T, Tag: TokenTag> TokenSlabVacantEntry<'_, T, Tag> {
    pub fn token(&self) -> Token {
        Token::from_key(Key::from_raw(self.0.key()))
    }
}

impl<T, Tag> TokenCellSlab<T, Tag> {
    #[must_use]
    pub fn with_capacity(capacity: TokenCapacity) -> Self {
        Self(CellSlab::with_capacity(capacity.get()))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = Key<Tag>> + '_ {
        self.0.keys().map(Key::from_raw)
    }

    pub fn insert(&self, value: T) -> Result<Key<Tag>, T> {
        self.0.insert(value).map(Key::from_raw)
    }

    pub fn update<R>(&self, key: Key<Tag>, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        self.0.update(key.raw(), f)
    }

    pub fn update_parts<R>(&self, parts: KeyParts<Tag>, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        self.0.update_parts(parts.raw(), f)
    }

    pub fn remove(&self, key: Key<Tag>) -> Option<T> {
        self.0.remove(key.raw())
    }

    pub fn remove_parts(&self, parts: KeyParts<Tag>) -> Option<T> {
        self.0.remove_parts(parts.raw())
    }

    pub fn remove_parts_with<R>(
        &self,
        parts: KeyParts<Tag>,
        f: impl FnOnce(&mut T) -> Option<R>,
    ) -> Option<(T, R)> {
        self.0.remove_parts_with(parts.raw(), f)
    }
}
