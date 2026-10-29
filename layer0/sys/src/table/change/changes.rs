use o3::alloc;

use crate::{Budget, table::change};

pub trait Changes: Sized {
    type Slots: super::raw::Slots;
    type Id: change::Id<Slot = <Self::Slots as super::raw::Slots>::Physical>;
    type Read<'a>: change::Interest<Id = Self::Id, Data = <Self::Slots as super::raw::Slots>::Data>
    where
        Self: 'a;
    type Write<'a>: change::Write
        + change::Interest<Id = Self::Id, Data = <Self::Slots as super::raw::Slots>::Data>
    where
        Self: 'a;
    type Tail<'a>: change::Tail
    where
        Self: 'a;
    type Deletion<'a>: change::Delete
    where
        Self: 'a;

    fn try_new(
        capacity: <Self::Slots as super::raw::Slots>::Capacity,
    ) -> Result<Self, alloc::Error>;
    fn read(&mut self, id: Self::Id) -> Option<Self::Read<'_>>;
    fn write(&mut self, id: Self::Id) -> Option<Self::Write<'_>>;
    fn try_delete(
        &mut self,
        entries: impl Iterator<
            Item = Result<
                Option<(Self::Id, change::Filter)>,
                change::Index<<Self::Slots as super::raw::Slots>::Index>,
            >,
        >,
    ) -> Result<Self::Deletion<'_>, change::Index<<Self::Slots as super::raw::Slots>::Index>>;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn batch(&mut self, budget: &mut impl Budget) -> Self::Tail<'_>;
    fn tail(&mut self, limit: usize) -> Self::Tail<'_>;
}
