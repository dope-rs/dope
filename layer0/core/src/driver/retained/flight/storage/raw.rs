use std::{cell, marker, mem::MaybeUninit, ops, ptr};

use o3::{
    alloc::{self, array::Plan, tail},
    depot::{arena::linked::chain, array::init},
};

use crate::driver::retained::flight::storage::{
    Flights, Preparation, Prepared, Slots, index_limit, is_layout_valid, state,
};

/// Directory topology changes only under the sole preparation capability.
#[repr(transparent)]
pub(super) struct Directory<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>(
    cell::UnsafeCell<Vec<ptr::NonNull<state::State<T, INDEX_BITS, GENERATION_BITS>>>>,
);

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Directory<T, INDEX_BITS, GENERATION_BITS>
{
    pub(super) const fn new() -> Self {
        Self(cell::UnsafeCell::new(Vec::new()))
    }

    pub(super) fn read(&self) -> &Vec<ptr::NonNull<state::State<T, INDEX_BITS, GENERATION_BITS>>> {
        // SAFETY: each private read ends before the callback-free commit leaf;
        // external completions borrow stable entries, never the directory itself.
        unsafe { &*self.0.get() }
    }
}

/// Exact arena identity plus the affine right to stage and publish one batch.
#[repr(transparent)]
pub(in crate::driver::retained::flight) struct Access<
    'a,
    'owner,
    T: Copy,
    const INDEX_BITS: u32,
    const GENERATION_BITS: u32,
> {
    arena: &'owner Flights<T, INDEX_BITS, GENERATION_BITS>,
    preparation: marker::PhantomData<&'a mut ()>,
}

impl<'a, 'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Access<'a, 'owner, T, INDEX_BITS, GENERATION_BITS>
{
    /// # Safety
    /// The exact arena stays alive and immovable for `'owner`; this capability
    /// exclusively owns preparation for `'a` until it is committed or dropped.
    pub(in crate::driver::retained::flight) unsafe fn new(
        arena: &'owner Flights<T, INDEX_BITS, GENERATION_BITS>,
    ) -> Self {
        Self {
            arena,
            preparation: marker::PhantomData,
        }
    }

    pub(in crate::driver::retained::flight) fn try_batch<const GROUPS: usize>(
        self,
        caps: [usize; GROUPS],
    ) -> Result<Prepared<'a, 'owner, T, INDEX_BITS, GENERATION_BITS, GROUPS>, alloc::Error> {
        const { assert!(GROUPS > 0) };
        if !is_layout_valid::<INDEX_BITS, GENERATION_BITS>() {
            return Err(alloc::Error::Overflow);
        }
        let base = self.arena.directory.read().len();
        let mut end = base;
        let mut rest = caps
            .iter()
            .try_fold(0usize, |total, capacity| total.checked_add(*capacity));
        let mut entries = MaybeUninit::uninit();
        let mut guard = init::Guard::<_, GROUPS, false>::new(&mut entries);
        let states = loop {
            let vacant = match guard.next() {
                ops::ControlFlow::Break(states) => break states.into_array(),
                ops::ControlFlow::Continue(vacant) => vacant,
            };
            let capacity = *vacant.get(&caps);
            rest = rest.map(|rest| rest - capacity);
            let state = match state::Header::select(&self.arena.groups, capacity) {
                Some(slots) => Preparation::Reuse(slots),
                None => {
                    let (seed, extent, next) = Self::seed(end, capacity, rest)?;
                    let block = state::Header::try_block(extent, seed)?;
                    end = next;
                    Preparation::New { block, capacity }
                }
            };
            guard = vacant.insert(state);
        };
        let added = end - base;
        let dir = self.arena.directory.read();
        let spare = dir.capacity() - dir.len();
        let next = if added > spare {
            Some(self.try_plan(end)?)
        } else {
            None
        };
        Ok(Prepared {
            access: self,
            states,
            directory: next,
        })
    }

    pub(super) fn commit<const GROUPS: usize>(
        self,
        states: [Preparation<'owner, T, INDEX_BITS, GENERATION_BITS>; GROUPS],
        next: Option<Vec<ptr::NonNull<state::State<T, INDEX_BITS, GENERATION_BITS>>>>,
    ) -> [Slots<'owner, T, INDEX_BITS, GENERATION_BITS>; GROUPS] {
        // SAFETY: this consumed capability owns the only outstanding plan for
        // this exact directory. No directory references escape its short reads,
        // and this leaf invokes no completion or user callbacks while mutating it.
        let dir = unsafe { &mut *self.arena.directory.0.get() };
        if let Some(next) = next {
            *dir = next;
        }
        states.map(|state| match state {
            Preparation::Reuse(slots) => slots,
            Preparation::New { block, capacity } => {
                Self::publish(&self.arena.groups, dir, block, capacity)
            }
        })
    }

    fn try_plan(
        &self,
        required: usize,
    ) -> Result<Vec<ptr::NonNull<state::State<T, INDEX_BITS, GENERATION_BITS>>>, alloc::Error> {
        let dir = self.arena.directory.read();
        let source = dir.as_slice();
        let mut next: Vec<ptr::NonNull<state::State<T, INDEX_BITS, GENERATION_BITS>>> =
            Plan::try_reserve(dir.capacity(), required)?.try_vec()?;
        // SAFETY: the plan retains at least the old capacity. The new allocation
        // is disjoint from this live directory, and copying initializes its prefix.
        unsafe {
            ptr::copy_nonoverlapping(source.as_ptr(), next.as_mut_ptr(), source.len());
            next.set_len(source.len());
        }
        Ok(next)
    }

    fn publish(
        groups: &'owner chain::List<
            state::Header<T, INDEX_BITS, GENERATION_BITS>,
            state::State<T, INDEX_BITS, GENERATION_BITS>,
        >,
        dir: &mut Vec<ptr::NonNull<state::State<T, INDEX_BITS, GENERATION_BITS>>>,
        block: tail::Block<
            chain::Node<
                state::Header<T, INDEX_BITS, GENERATION_BITS>,
                state::State<T, INDEX_BITS, GENERATION_BITS>,
            >,
            state::State<T, INDEX_BITS, GENERATION_BITS>,
        >,
        capacity: usize,
    ) -> Slots<'owner, T, INDEX_BITS, GENERATION_BITS> {
        let len = dir.len();
        let slots = state::Header::publish(groups, block, capacity);
        let entries = slots.group.as_slice();
        let target = dir.as_mut_ptr();
        for (offset, entry) in entries.iter().enumerate() {
            // SAFETY: the sole plan reserved this exact arena's complete growth.
            unsafe { target.add(len + offset).write(ptr::NonNull::from(entry)) };
        }
        // SAFETY: every new directory entry above is initialized and in capacity.
        unsafe { dir.set_len(len + entries.len()) };
        slots
    }

    /// Admits exact capacity and rounds only within room left for the batch.
    fn seed(
        len: usize,
        capacity: usize,
        rest: Option<usize>,
    ) -> Result<(u32, usize, usize), alloc::Overflow> {
        let required = len.checked_add(capacity).ok_or(alloc::Overflow)?;
        let limit = index_limit::<INDEX_BITS>();
        if required > limit {
            return Err(alloc::Overflow);
        }
        let extent = match rest {
            Some(rest) if capacity != 0 => capacity
                .checked_next_power_of_two()
                .filter(|extent| *extent <= (limit - len).saturating_sub(rest))
                .unwrap_or(capacity),
            Some(_) | None => capacity,
        };
        Ok((len as u32, extent, len + extent))
    }
}
