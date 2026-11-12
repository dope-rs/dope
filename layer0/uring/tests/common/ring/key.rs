use dope_sys::table::permit;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Key(pub u64);

// SAFETY: the wrapped scalar is the complete generation identity.
unsafe impl permit::raw::Key for Key {
    fn identity(self) -> u64 {
        self.0
    }
}
