use dope_core::driver::schedule::ready::task::topology;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Index(pub u32);

unsafe impl topology::raw::Index for Index {
    fn raw(self) -> u32 {
        self.0
    }

    unsafe fn from_raw(raw: u32) -> Self {
        Self(raw)
    }
}
