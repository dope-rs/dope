use crate::wire::send;

pub trait Plain<'a> {
    /// Constructs a retained direct-send view.
    /// # Safety
    /// The bytes stay fixed through completion or quiescence.
    unsafe fn retain(self) -> send::Plain<'a>;
}

impl<'a> Plain<'a> for &'a [u8] {
    unsafe fn retain(self) -> send::Plain<'a> {
        send::Plain::proven(self)
    }
}
