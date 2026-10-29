//! Client wire-failure scenarios.

pub mod capacity;
pub mod late;
pub mod open;
pub mod park;
pub mod read;
pub mod send;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error<'d> {
    Protocol(&'d str),
    Truncated(&'d str),
}

#[derive(Clone, Copy)]
pub struct Context<'d>(pub(crate) &'d str);

impl Default for Context<'_> {
    fn default() -> Self {
        Self("domain-borrowed terminal failure")
    }
}
