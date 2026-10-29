use std::{marker, ops};

use dope::net::{
    wire,
    wire::receive::{Chunk, mode::access::Use},
};
use o3::buffer::write::consume;

/// A retained cursor that exclusively borrows its `Io` until it is dropped.
///
/// ```compile_fail
/// use dope::net::wire::Spec;
/// use dope_fiber::net::read::Lease;
///
/// fn widen<'io, 'w, 'd: 'w, W: Spec + 'w>(
///     lease: Lease<'io, 'w, 'd, W>,
/// ) -> Lease<'static, 'w, 'd, W> {
///     lease
/// }
/// ```
#[must_use = "dropping a read lease discards unread bytes and returns receive credit"]
pub struct Lease<'io, 'w, 'd: 'w, W: wire::Spec + 'w> {
    cursor: Chunk<'w, 'd, W>,
    _io: marker::PhantomData<&'io mut ()>,
}

impl<'io, 'w, 'd: 'w, W: wire::Spec + 'w> Lease<'io, 'w, 'd, W> {
    pub(crate) fn new(cursor: Chunk<'w, 'd, W>) -> Self {
        Self {
            cursor,
            _io: marker::PhantomData,
        }
    }

    /// Visits the cursor while retaining its connection borrow and receive credit.
    pub fn drive<T>(&mut self, done: T, use_head: &mut impl Use<'d, T>) -> T {
        self.cursor.drive(done, use_head)
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> ops::Deref for Lease<'_, 'w, 'd, W> {
    type Target = Chunk<'w, 'd, W>;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl<'w, 'd: 'w, W: wire::Spec + 'w> consume::Consume for Lease<'_, 'w, 'd, W>
where
    Chunk<'w, 'd, W>: consume::Consume,
{
    fn remaining(&self) -> usize {
        self.cursor.remaining()
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        self.cursor.try_consume(amount)
    }

    fn consume_max(&mut self, max: usize) -> usize {
        self.cursor.consume_max(max)
    }
}
