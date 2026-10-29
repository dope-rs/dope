use o3::buffer;

use crate::memory;

type Extent<P> = <P as Pending>::Extent;
type Rejection<P> = (
    P,
    <Extent<P> as memory::raw::Extent>::Length,
    <Extent<P> as memory::raw::Extent>::Capacity,
);

/// A unique completion transfers one selected slot from its original ring.
pub trait Select {
    type Index: Copy;
    type Pending: Token<Index = Self::Index>
        + Pending<
            Complete: Token<
                Index = Self::Index,
                Length = <Extent<Self::Pending> as memory::raw::Extent>::Length,
            >,
        >;

    /// # Safety
    /// A unique CQE transfers any valid reported slot from this ring.
    /// Keep its payload live; project and return only through its original context/ring.
    unsafe fn select(&self, index: Self::Index) -> Result<Self::Pending, Self::Index>;
}

/// # Safety
/// The index names this owner's unique live slot and cannot change or duplicate.
/// Its length denotes a live prefix wholly contained within that same slot.
pub unsafe trait Token {
    type Index: Copy;
    type Length: Copy;

    fn index(&self) -> Self::Index;
    fn length(&self) -> Self::Length;
}

/// Returns a receive slot to its original native storage owner.
pub trait Release<T> {
    /// # Safety
    /// The buffer is unique, has no live projections, and was issued by this owner.
    /// Retain that storage until the return is consumed or published.
    unsafe fn release(&mut self, buffer: T);
}

/// Projects a completed prefix from its original fixed-residency payload allocation.
pub trait Context<T: Token>: buffer::raw::Fixed<T> {
    /// # Safety
    /// The completed token belongs to this exact context and retains its selected slot.
    /// Its initialized prefix remains immutable and unavailable to the producer until the borrow ends.
    unsafe fn bytes<'a>(&'a self, buffer: &'a T) -> &'a [u8];
}

/// A selected slot whose completed prefix has not yet been admitted.
pub trait Pending: Sized {
    type Extent: memory::raw::Extent;
    type Complete;

    fn try_complete(
        self,
        len: <Self::Extent as memory::raw::Extent>::Length,
    ) -> Result<Self::Complete, Rejection<Self>>;
}
