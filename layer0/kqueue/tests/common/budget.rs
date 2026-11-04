use o3::mem::quota;

pub struct Budget<'a>(quota::Lease<'a, ()>);

impl<'a> Budget<'a> {
    pub fn new(source: &'a quota::Ledger) -> Self {
        Self::limited(source, usize::MAX)
    }

    pub fn limited(source: &'a quota::Ledger, limit: usize) -> Self {
        Self(quota::Lease::reserve_up(source, limit))
    }
}

impl dope_sys::Budget for Budget<'_> {
    fn remaining(&self) -> usize {
        self.0.remaining()
    }

    fn admit<T>(&mut self, acquire: impl FnOnce() -> Option<T>) -> quota::Admission<T> {
        self.0.admit_with(acquire)
    }
}
