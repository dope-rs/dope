use std::cell::Cell;

pub struct Owner<'a> {
    pub drops: &'a Cell<Option<usize>>,
    pub value: u64,
}

impl Drop for Owner<'_> {
    fn drop(&mut self) {
        self.drops
            .set(self.drops.get().and_then(|drops| drops.checked_add(1)));
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Rejected(pub u64);
