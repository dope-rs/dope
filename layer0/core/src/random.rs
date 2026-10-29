//! OS-provided seed material retained by a driver.

use std::mem;

use dope_sys::thread::Thread as _;

/// Two seed words acquired from the selected operating-system backend.
#[repr(transparent)]
pub struct Seed([u64; 2]);

impl Seed {
    pub fn acquire() -> Result<Self, crate::Errno> {
        let mut bytes = mem::MaybeUninit::uninit();
        let [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p] =
            *crate::Backend::entropy::<16>(&mut bytes)?;
        Ok(Self([
            u64::from_ne_bytes([a, b, c, d, e, f, g, h]),
            u64::from_ne_bytes([i, j, k, l, m, n, o, p]),
        ]))
    }

    /// Applies the owning layer's key derivation without adding seed storage.
    pub fn map(&self, derive: impl FnOnce([u64; 2]) -> [u64; 2]) -> Self {
        Self(derive(self.0))
    }
}

impl AsRef<[u64; 2]> for Seed {
    fn as_ref(&self) -> &[u64; 2] {
        &self.0
    }
}
