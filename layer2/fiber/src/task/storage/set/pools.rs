use core::cell;

use o3::{
    alloc,
    alloc::tail,
    depot::{capacity, queue::indexed::keyed::lanes},
};

use crate::{
    abi,
    task::{
        storage,
        storage::{
            Handle,
            set::{Allocation, Borrowed, Length, entries::Entries, ready, slot, views},
        },
    },
};

pub(in crate::task::storage) struct Header<'d, F: abi::Fiber<'d>, M, D, G = ()> {
    pub(in crate::task::storage::set) ready: ready::Queue<'d, F, M, D, G>,
    len: cell::Cell<Length>,
    pub(in crate::task::storage::set) free: cell::Cell<Option<ready::raw::Index<D>>>,
}

impl<'d, F: abi::Fiber<'d>, M, D, G> Header<'d, F, M, D, G> {
    fn new(cap: capacity::Count, work: lanes::Fifo<Handle<'d, D>, 3, storage::Id<'d, D>>) -> Self {
        Self {
            ready: ready::Queue::new(work),
            len: cell::Cell::new(Length::ZERO),
            free: cell::Cell::new(
                (cap.raw() != 0).then(|| unsafe { ready::raw::Index::from_offset(0) }),
            ),
        }
    }

    fn len(&self) -> Length {
        self.len.get()
    }

    /// Consumes an acquired vacancy, proving occupancy is below the admitted capacity.
    pub(in crate::task::storage::set) fn admit<'a>(
        &self,
        admitted: slot::Admitted<'a, 'd, F, M, D, G>,
    ) -> slot::Counted<'a, 'd, F, M, D, G> {
        let raw = self.len.get().raw();
        let raw = raw + 1;
        self.len.set(Length::new(raw));
        slot::Counted::new(admitted)
    }

    /// Consumes one occupied slot, proving the current occupancy is nonzero.
    pub(in crate::task::storage::set) fn retire<'a>(
        &self,
        vacant: slot::Vacated<'a, 'd, F, M, D, G>,
    ) -> slot::Vacated<'a, 'd, F, M, D, G> {
        let raw = self.len.get().raw();
        let raw = raw - 1;
        self.len.set(Length::new(raw));
        vacant
    }
}

pub(super) struct State<'d, F: abi::Fiber<'d>, M, D, G = ()> {
    slots: Allocation<'d, F, M, D, G>,
}

impl<'d, F: abi::Fiber<'d>, M, D, G> State<'d, F, M, D, G> {
    pub(in crate::task::storage::set) fn try_new(
        capacity: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        let work = lanes::Fifo::<Handle<'d, D>, 3, storage::Id<'d, D>>::try_new(capacity)?;
        let memory = tail::Plan::try_new(capacity)?.try_allocate()?;
        Ok(Self {
            slots: memory.fill(Header::new(capacity, work), |_, index| {
                let next = (index + 1 < capacity.raw())
                    .then(|| unsafe { ready::raw::Index::from_offset(index + 1) });
                slot::Slot::new(unsafe { ready::raw::Index::from_offset(index) }, next)
            }),
        })
    }

    pub(in crate::task::storage::set) fn view(&self) -> views::View<'_, 'd, F, M, D, G> {
        views::View::new(&self.slots)
    }

    pub(in crate::task::storage::set) fn entries(&mut self) -> Entries<'_, 'd, F, M, D, G> {
        Entries::new(&mut self.slots)
    }

    pub(in crate::task::storage::set) fn len(&self) -> Length {
        Borrowed::new(&self.slots).header().len()
    }
}

impl<'d, F: abi::Fiber<'d>, M, D, G> Drop for State<'d, F, M, D, G> {
    fn drop(&mut self) {
        if self.len() != Length::ZERO {
            self.entries().clear();
        }
    }
}
