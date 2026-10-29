use o3::alloc;

use crate::driver::retained::flight::storage::{self, Drain, Flights, Prepared};

/// Sole preparation authority for one borrowed flight arena.
#[repr(transparent)]
pub struct Builder<'owner, T: Copy, const INDEX_BITS: u32 = 32, const GENERATION_BITS: u32 = 32> {
    pub(super) arena: &'owner Flights<T, INDEX_BITS, GENERATION_BITS>,
}

impl<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Builder<'owner, T, INDEX_BITS, GENERATION_BITS>
{
    /// Keeps this builder borrowed until the batch is committed or rolled back.
    pub fn try_batch<const GROUPS: usize>(
        &mut self,
        caps: [usize; GROUPS],
    ) -> Result<Prepared<'_, 'owner, T, INDEX_BITS, GENERATION_BITS, GROUPS>, alloc::Error> {
        // SAFETY: the arena's exclusive owner borrow created this sole builder;
        // Prepared retains this mutable borrow until commit or rollback.
        let access = unsafe { storage::raw::Access::new(self.arena) };
        access.try_batch(caps)
    }

    /// Completion borrows the arena independently of the next preparation.
    pub fn drain(&self) -> Drain<'owner, T, INDEX_BITS, GENERATION_BITS> {
        self.arena.drain()
    }
}
