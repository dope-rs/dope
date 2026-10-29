use std::{marker, num};

use o3::{alloc, base::marker::ThreadBound, depot::arena::linked::chain};

use crate::{
    driver,
    driver::{
        receive::{
            credit,
            credit::{storage, storage::Entry},
        },
        route::table::conn,
        schedule::ready,
        settings,
    },
};

/// Owner of exact reusable linear retention groups.
pub struct Owner<T: Copy, S: Copy, C> {
    context: C,
    groups: chain::List<storage::Group<C>, Entry<T, S, C>>,
    thread: marker::PhantomData<ThreadBound>,
}

impl<T: Copy, S: Copy, C> Owner<T, S, C> {
    pub const fn new(context: C) -> Self {
        Self {
            context,
            groups: chain::List::new(),
            thread: marker::PhantomData,
        }
    }

    pub fn context(&self) -> &C {
        &self.context
    }

    pub fn try_slots(
        &self,
        capacity: num::NonZero<u32>,
    ) -> Result<storage::Slots<'_, T, S, C>, alloc::Error> {
        let vacant = match self.select(capacity) {
            Some(vacant) => vacant,
            None => storage::Vacant::allocate(&self.groups, capacity, &self.context)?,
        };
        Ok(storage::Slots::new(vacant.activate(&self.context)))
    }

    fn select(&self, required: num::NonZero<u32>) -> Option<storage::Vacant<'_, T, S, C>> {
        let mut selected: Option<storage::Vacant<'_, T, S, C>> = None;
        for raw in self.groups.iter() {
            let Some(vacant) = storage::Vacant::select(raw, required) else {
                continue;
            };
            if vacant.is_exact() {
                return Some(vacant);
            }
            if selected
                .as_ref()
                .is_none_or(|current| vacant.is_smaller(current))
            {
                selected = Some(vacant);
            }
        }
        selected
    }
}

impl Owner<ready::Stored, credit::Wake, ready::Arena> {
    pub(in crate::driver) fn try_new(plan: settings::Plan) -> Result<Self, alloc::Error> {
        Ok(Self::new(ready::Arena::new(plan)?))
    }

    pub(in crate::driver) fn try_storage<'d>(
        &'d self,
        cap: conn::Capacity,
        driver: driver::Invariant<'d>,
    ) -> Result<credit::Storage<'d>, alloc::Error> {
        let entries = self.try_slots(cap.count())?;
        Ok(credit::Storage { entries, driver })
    }
}

const _: () = {
    use std::{mem, num};

    assert!(
        mem::size_of::<Owner<num::NonZero<u64>, num::NonZero<u8>, ()>>() == mem::size_of::<usize>()
    );
};
