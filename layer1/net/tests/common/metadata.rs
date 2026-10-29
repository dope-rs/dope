use std::cell::Cell;

use o3::buffer::Residency;

pub struct Counted<'a>(pub &'a Cell<Option<u32>>);

impl Drop for Counted<'_> {
    fn drop(&mut self) {
        self.0
            .set(self.0.get().and_then(|count| count.checked_add(1)));
    }
}

pub struct Owner {
    pub resident: usize,
}

impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        b"x"
    }
}

impl Residency for Owner {
    fn resident(&self) -> usize {
        self.resident
    }
}
