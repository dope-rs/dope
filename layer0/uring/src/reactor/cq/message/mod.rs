use dope_sys::io::reactor::complete::message;

mod capacity;
mod length;
mod output;
mod span;

pub(crate) use capacity::Capacity;
pub(crate) use length::Length;
pub(crate) use output::Output;
use span::Span;

type Error = message::Error<Capacity, Length>;
