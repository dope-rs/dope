use std::cell;

#[repr(align(128))]
#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub struct Key(pub(crate) u8);

#[repr(align(128))]
pub struct Value<'a>(pub(crate) &'a cell::Cell<usize>);

impl Drop for Value<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}
