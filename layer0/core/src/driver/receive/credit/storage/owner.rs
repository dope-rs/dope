use std::{marker, num, ptr};

use o3::{alloc, alloc::tail, base::marker::ThreadBound, depot::arena::linked::chain};

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
        let count = capacity.get() as usize;
        let mut selected: Option<(chain::Link<'_, _, _>, _)> = None;
        for raw in self.groups.iter() {
            if !raw.header().available.get() {
                continue;
            }
            let Some(entries) = raw.as_slice().get(..count) else {
                continue;
            };
            if selected
                .as_ref()
                .is_none_or(|(current, _)| raw.len() < current.len())
            {
                selected = Some((raw, entries));
            }
            if raw.len() == count {
                break;
            }
        }
        let (raw, entries) = match selected {
            Some((raw, entries)) => {
                let group = raw.header();
                group.available.set(false);
                group.context.set(ptr::NonNull::from(&self.context));
                for entry in entries {
                    entry.state.set(storage::State::Idle);
                }
                group.attachment.set(storage::Attachment::Active);
                (raw, entries)
            }
            None => {
                // Geometric backing bounds history retained by old keys until owner drop.
                let extent = count.checked_next_power_of_two().ok_or(alloc::Overflow)?;
                let memory = tail::Plan::try_new(extent)?.try_allocate()?;
                let block = memory.fill(
                    chain::Node::new(storage::Group::new(&self.context)),
                    |node, _| Entry::new(node.header()),
                );
                let raw = self.groups.push(block);
                // SAFETY: checked growth bounds count by this initialized allocation;
                // the immutable owner borrow retains it through every issued slot.
                (raw, unsafe { raw.as_slice().get_unchecked(..count) })
            }
        };
        Ok(storage::Slots {
            group: raw.header(),
            entries,
            capacity,
        })
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
