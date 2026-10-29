use dope_core::io::transfer;
use o3::{
    buffer,
    buffer::{Residency, bytes::bounded::Overflow, pool, storage},
};

use crate::link::egress::{self, NonEmpty};

/// Exact borrowed suffixes of a prefix/body payload.
pub struct Cursor<'a> {
    pub(in crate::link::egress::data) prefix: &'a [u8],
    pub(in crate::link::egress::data) body: &'a [u8],
}

const _: () = assert!(size_of::<Cursor<'static>>() == 4 * size_of::<usize>());

/// One immutable pooled prefix followed by its retained body.
pub struct Source<C> {
    pub(in crate::link::egress::data) prefix: pool::Cursor,
    pub(in crate::link::egress::data) body: C,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Prefix(Overflow),
    Body(Overflow),
    Wire(Overflow),
    Empty(storage::Zero),
    PrefixResident(egress::ResidentOut),
    BodyResident(egress::ResidentOut),
    Resident(egress::Overflow<egress::Resident>),
}

/// Initial validation retains both owners and the exact failing component.
pub struct Rejected<C> {
    prefix: pool::Cursor,
    body: C,
    cause: Error,
}

impl<C> Rejected<C> {
    pub fn into_parts(self) -> (pool::Cursor, C, Error) {
        (self.prefix, self.body, self.cause)
    }
}

impl<C: buffer::raw::Segment> Source<C> {
    pub fn try_new(prefix: pool::Cursor, body: C) -> Result<NonEmpty<Self>, Rejected<C>> {
        let geometry = (|| {
            let left = transfer::Len::try_new(prefix.len()).map_err(Error::Prefix)?;
            let right = transfer::Len::try_new(body.as_ref().len()).map_err(Error::Body)?;
            let wire = left.checked_add(right).map_err(Error::Wire)?;
            let wire = transfer::NonZero::try_from(wire).map_err(Error::Empty)?;
            let left =
                egress::Resident::try_from(prefix.resident()).map_err(Error::PrefixResident)?;
            let right = egress::Resident::try_from(body.resident()).map_err(Error::BodyResident)?;
            let resident = left.checked_add(right).map_err(Error::Resident)?;
            Ok((wire, resident))
        })();
        match geometry {
            Ok((wire, resident)) => Ok(NonEmpty::new(Self { prefix, body }, wire, resident)),
            Err(cause) => Err(Rejected {
                prefix,
                body,
                cause,
            }),
        }
    }

    pub fn into_parts(self) -> (pool::Cursor, C) {
        (self.prefix, self.body)
    }
}

const _: () = {
    assert!(
        size_of::<Source<storage::Shared>>()
            == size_of::<pool::Cursor>() + size_of::<storage::Shared>()
    );
    assert!(
        size_of::<NonEmpty<Source<storage::Shared>>>() == size_of::<Source<storage::Shared>>() + 8
    );
};
