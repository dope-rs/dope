use dope_sys::io::socket::addr;

mod sealed;

pub(crate) use sealed::Sealed;

use crate::io::socket::StreamSpec;

/// Validated, byte-stable storage for a retained socket operation.
pub trait Prepared: Sealed + Copy {
    fn view(&self) -> View<'_>;

    fn stream_spec(&self) -> StreamSpec;
}

/// A validated address borrowed for one socket operation.
pub struct View<'a>(pub(crate) addr::Addr<'a>);

impl<'a> View<'a> {
    pub(crate) fn native(self) -> addr::Addr<'a> {
        self.0
    }
}
