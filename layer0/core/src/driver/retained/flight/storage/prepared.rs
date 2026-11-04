use std::{marker, ptr};

use crate::driver::retained::flight::storage::{Flights, Preparation, state};

/// Fully staged flight acquisition retaining its exact arena and preparation loan.
#[must_use = "prepared flight groups must be committed"]
pub struct Prepared<
    'a,
    'owner,
    T: Copy,
    const INDEX_BITS: u32 = 32,
    const GENERATION_BITS: u32 = 32,
    const GROUPS: usize = 1,
> {
    pub(super) arena: &'owner Flights<T, INDEX_BITS, GENERATION_BITS>,
    pub(super) preparation: marker::PhantomData<&'a mut ()>,
    pub(super) states: [Preparation<'owner, T, INDEX_BITS, GENERATION_BITS>; GROUPS],
    pub(super) directory: Option<Vec<ptr::NonNull<state::State<T, INDEX_BITS, GENERATION_BITS>>>>,
}
