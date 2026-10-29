use crate::table::reclaim::{Item, Kind};

/// The owning descriptor domain commits only consumed retirement authority.
pub trait View: Sized {
    type Slot;
    type Flight;
    type Close: Item<Slot = Self::Slot, Flight = Self::Flight>;
    type Retire: Item<Slot = Self::Slot, Flight = Self::Flight>;

    fn submitted(&self, flight: Self::Flight);
    fn commit(self, item: Kind<Self::Close, Self::Retire>);
}
