use std::slice;

use crate::reactor::Kernel;

#[repr(transparent)]
pub struct Events<'a>(pub(in crate::reactor) slice::Iter<'a, libc::kevent>);

impl<'a> Iterator for Events<'a> {
    type Item = Kernel<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Kernel)
    }
}
