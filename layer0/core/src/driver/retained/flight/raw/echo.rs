use crate::driver::retained::flight::{GENERATION_BITS, INDEX_BITS, Target, storage};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Echo(storage::Echo<Target, INDEX_BITS, GENERATION_BITS>);

impl From<Echo> for u64 {
    fn from(echo: Echo) -> Self {
        echo.0.expose()
    }
}

impl From<Echo> for usize {
    fn from(echo: Echo) -> Self {
        const { assert!(usize::BITS >= u64::BITS) };
        u64::from(echo) as usize
    }
}

impl Echo {
    pub(crate) const BITS: u32 = INDEX_BITS + GENERATION_BITS;

    pub(in crate::driver::retained::flight) const fn from_inner(
        echo: storage::Echo<Target, INDEX_BITS, GENERATION_BITS>,
    ) -> Self {
        Self(echo)
    }

    pub(in crate::driver::retained::flight) const fn into_inner(
        self,
    ) -> storage::Echo<Target, INDEX_BITS, GENERATION_BITS> {
        self.0
    }

    /// # Safety
    /// `raw` must be returned user data from the native reactor whose flight
    /// arena will resolve this echo; decoding preserves that arena's identity.
    pub(crate) unsafe fn from_kernel(raw: u64) -> Option<Self> {
        Some(Self(storage::Echo::from_exposed(raw)?))
    }
}
