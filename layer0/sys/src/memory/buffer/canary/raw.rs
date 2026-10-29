use crate::{
    io::{Close, Retry},
    memory,
    memory::buffer::{canary::Failure, provided},
};

type Extent<S> = <<S as Storage>::Layout as memory::raw::Receive>::Extent;
type Inspection<S, E> = Failure<
    <S as Storage>::Index,
    <Extent<S> as memory::raw::Extent>::Length,
    <Extent<S> as memory::raw::Extent>::Capacity,
    E,
>;

/// One owner retains canary payloads, registration entries, and their scalar.
pub trait Storage: Close<State: Retry + memory::Failure> + Sized {
    type Layout: memory::raw::Canary;
    type Index: Copy;
    type Error: provided::Failure<Cleanup = Self::State>;
    type Buffer<const GROUP: u16>;

    fn new() -> Result<Self, Self::Error>;
    fn into_buffer<const GROUP: u16>(self) -> Self::Buffer<GROUP>;

    /// # Safety
    /// No kernel operation or other borrower may access the payload scalar.
    unsafe fn option(&mut self) -> &libc::c_int;

    /// # Safety
    /// A unique CQE transfers any valid reported slot; no other access remains.
    unsafe fn inspect<E>(
        &mut self,
        index: Self::Index,
        len: <<Self::Layout as memory::raw::Receive>::Extent as memory::raw::Extent>::Length,
        check: impl FnOnce(&[u8]) -> Result<(), E>,
    ) -> Result<(), Inspection<Self, E>>;
}
