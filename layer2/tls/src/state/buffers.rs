use crate::error;
use dope::net;
use o3::{
    alloc,
    buffer::{pool, pool::owners, storage},
};

/// Exclusive receive storage transferred from pool admission to one read.
#[repr(transparent)]
pub(crate) struct Reserve<'d>(pool::Span<owners::Plain, pool::Borrowed<'d>>);

impl<'d> Reserve<'d> {
    pub(super) fn copy(
        mut self,
        plain: &[u8],
    ) -> Result<storage::Shared<owners::Edge<owners::Plain, pool::Borrowed<'d>>>, error::Error>
    {
        self.0
            .try_append(plain)
            .map_err(|cause| error::Error::Consume {
                buffer: net::BufferKind::Receive,
                cause,
            })?
            .commit();
        Ok(self.0.freeze())
    }
}

pub(crate) enum Receive {
    Fixed(pool::Pool),
    Growing(pool::Growing),
}

impl Receive {
    pub(crate) fn try_reserve(&self) -> Result<Option<Reserve<'_>>, alloc::Exhausted> {
        let recv = match self {
            Self::Fixed(pool) => pool.borrow(),
            Self::Growing(pool) => pool.try_borrow()?,
        };
        Ok(recv.map(Reserve))
    }
}
