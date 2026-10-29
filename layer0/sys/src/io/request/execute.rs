use crate::io::request::Complete;

/// Executes a retained file request before consuming its completion authority.
pub trait Execute {
    fn execute<C: Complete>(self, done: C) -> C::Output;
}
