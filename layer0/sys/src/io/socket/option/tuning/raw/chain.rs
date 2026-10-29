use crate::io::{reactor::complete, socket::option};

/// A reserved submission span for one pinned option chain.
pub trait Chain<T: complete::raw::Token, const N: usize>: Sized {
    type Target;

    /// # Safety
    /// The cursor contains the reserved count. Every scalar address must remain
    /// pinned and live until every option completion is consumed or queue quiescence.
    unsafe fn submit(
        self,
        values: &mut [libc::c_int; N],
        options: impl option::Cursor,
        slot: Self::Target,
        token: T,
    );
}
