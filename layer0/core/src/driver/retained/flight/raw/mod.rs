use std::{mem, ptr};

pub(crate) use echo::Echo;
use o3::alloc;

mod echo;

use flight::{GENERATION_BITS, INDEX_BITS, Target, storage};

use crate::driver::{self, retained::flight};

/// Exact driver arena with its short exclusive preparation loan.
pub(super) struct Access<'a, 'd> {
    arena: storage::raw::Access<'a, 'd, Target, INDEX_BITS, GENERATION_BITS>,
    driver: driver::Invariant<'d>,
}

impl<'a, 'd> Access<'a, 'd> {
    pub(super) fn new(
        arena: &'a mut storage::Flights<Target, INDEX_BITS, GENERATION_BITS>,
        driver: driver::Reference<'d>,
    ) -> Self {
        let pointer = ptr::NonNull::from(arena);
        // SAFETY: Context supplies this exact driver's arena, which remains
        // pinned and live for 'd. The input's exclusive preparation loan is
        // retained by Access and Prepared for 'a. Only shared arena access is
        // extended; topology mutation stays inside the directory's UnsafeCell.
        let arena = unsafe { storage::raw::Access::new(pointer.as_ref()) };
        Self {
            arena,
            driver: driver.brand,
        }
    }

    pub(super) fn try_batch<const GROUPS: usize>(
        self,
        caps: [usize; GROUPS],
    ) -> Result<
        (
            storage::Prepared<'a, 'd, Target, INDEX_BITS, GENERATION_BITS, GROUPS>,
            driver::Invariant<'d>,
        ),
        alloc::Error,
    > {
        Ok((self.arena.try_batch(caps)?, self.driver))
    }
}

const _: () = {
    assert!(mem::size_of::<Access<'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Access<'static, 'static>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Echo>() == mem::size_of::<usize>());
};
