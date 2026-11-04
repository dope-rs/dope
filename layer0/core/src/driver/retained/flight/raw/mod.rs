use std::{marker, ptr};

pub(crate) use echo::Echo;
use o3::alloc;

mod echo;

use flight::{GENERATION_BITS, INDEX_BITS, Target, storage};

use crate::driver::{self, retained::flight};

/// Prepares this driver's arena while retaining its exclusive preparation loan.
pub(super) fn prepare<'a, 'd, Tags, const GROUPS: usize>(
    arena: &'a mut storage::Flights<Target, INDEX_BITS, GENERATION_BITS>,
    need: flight::Need<Tags, GROUPS>,
    driver: driver::Reference<'d>,
) -> Result<flight::Prepared<'a, 'd, Tags, GROUPS>, alloc::Error> {
    let pointer = ptr::NonNull::from(arena);
    // SAFETY: Context supplies this exact driver's arena, which remains
    // pinned and live for 'd. The input's exclusive preparation loan is
    // retained by Prepared for 'a. Only shared arena access is
    // extended; topology mutation stays inside the directory's UnsafeCell.
    let prepared = unsafe {
        pointer
            .as_ref()
            .try_batch(need.into_caps().map(flight::Capacity::get))?
    };
    Ok(flight::Prepared {
        prepared,
        driver: driver.brand,
        tags: marker::PhantomData,
    })
}
