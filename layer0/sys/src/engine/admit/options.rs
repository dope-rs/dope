use crate::{
    Errno,
    engine::admit::Context,
    io::{request, socket::option},
};

/// Applies readiness options while retaining the reserved submission cell.
pub trait Options<C: Context> {
    fn set<O: option::Cursor>(
        &self,
        target: <C::Inputs as request::Context>::Target,
        options: Option<O>,
    ) -> Result<(), Errno>;
}
