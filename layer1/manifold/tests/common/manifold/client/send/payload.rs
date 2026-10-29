use std::cell::Cell;

pub struct Payload<'a>(pub(crate) &'a Cell<u8>);

impl Drop for Payload<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_add(1));
    }
}
