use std::cell::Cell;

pub struct Context<'a>(pub &'a Cell<usize>);

impl Drop for Context<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}
