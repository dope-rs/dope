use crate::{engine::admit::Context, io::request};

type Request<C> = <<C as Context>::Native as request::Source<<C as Context>::Inputs>>::Raw;

/// A reserved native submission slot, consumed before the host commits its flight.
pub trait Guard<C: Context>: Sized {
    fn submit(self, request: Request<C>, key: C::Key);
}
