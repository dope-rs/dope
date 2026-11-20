use o3::buffer::bytes;

use crate::{
    engine::admit::Context,
    io::{self, file, request, socket::addr},
};

type Slot<C> = <<<C as Context>::Native as request::Source<<C as Context>::Inputs>>::Raw as request::Lower>::Target;
type File<'a, C> = <<C as Context>::Native as file::File>::Request<'a>;

/// Consumes the exact native requests whose pointees remain owned by the host.
pub trait Retain<C: Context>: Sized {
    /// # Safety
    /// Retain the descriptor and bytes through terminal completion or quiescence.
    unsafe fn send(
        self,
        slot: Slot<C>,
        input: bytes::Prefix<'_, { i32::MAX as usize }>,
        key: C::Key,
    );

    /// # Safety
    /// Retain the descriptor and the complete message graph through completion or quiescence.
    unsafe fn send_msg<'a>(self, slot: Slot<C>, message: impl io::raw::Message<'a>, key: C::Key);

    /// # Safety
    /// Retain the listener and exclusive peer output through completion or quiescence.
    unsafe fn accept(self, listener: Slot<C>, peer: &mut addr::Storage, key: C::Key);

    /// # Safety
    /// Retain the descriptor and address through completion or quiescence.
    unsafe fn connect(self, slot: Slot<C>, address: addr::Addr<'_>, key: C::Key);

    /// # Safety
    /// Every file request borrow remains live through terminal completion or quiescence.
    unsafe fn file(self, request: File<'_, C>, key: C::Key);
}
