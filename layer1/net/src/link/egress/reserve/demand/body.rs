use std::{marker, mem};

use dope_core::io::transfer;
use o3::buffer::Residency;

use crate::link::egress::{Resident, load};

/// Admitted nonempty payload length for output planning.
/// The caller retains the source; final admission validates the transferred owner.
pub struct Body<B> {
    pub(super) len: transfer::NonZero,
    owner: marker::PhantomData<fn(B) -> B>,
}

impl<B> Body<B> {
    pub fn try_new(owner: &B) -> Result<Self, load::Error>
    where
        B: AsRef<[u8]> + Residency,
    {
        let len = transfer::Len::try_new(owner.as_ref().len()).map_err(load::Error::Transfer)?;
        let len = transfer::NonZero::try_from(len).map_err(load::Error::Empty)?;
        Resident::try_from(owner.resident()).map_err(load::Error::Resident)?;
        Ok(Self {
            len,
            owner: marker::PhantomData,
        })
    }
}

const _: () = {
    assert!(mem::size_of::<Body<()>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Option<Body<()>>>() == mem::size_of::<Body<()>>());
};
