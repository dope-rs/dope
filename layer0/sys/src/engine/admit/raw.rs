use crate::{
    engine::admit::Context,
    io::{file, request},
};

type Borrowed<'a, C> = <<<C as Context>::Native as request::Source<
    <C as Context>::Inputs,
>>::Raw as request::Lower>::Borrowed<'a>;
type File<'a, C> = <<C as Context>::Native as file::File>::Request<'a>;

/// Consumes the exact native requests whose pointees remain owned by the host.
pub trait Retain<C: Context>: Sized {
    /// # Safety
    /// Every encoded address remains live through terminal completion or quiescence.
    unsafe fn retain(self, request: Borrowed<'_, C>, key: C::Key);

    /// # Safety
    /// Every file request borrow remains live through terminal completion or quiescence.
    unsafe fn file(self, request: File<'_, C>, key: C::Key);
}
