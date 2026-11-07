use crate::{
    Errno,
    driver::route::kind,
    io::{event::entry::Entry, transfer::Len},
};

/// Exact result of one file read into its retained owner's output.
pub type Completion<'d> = Entry<'d, kind::Read, Result<Len, Errno>>;
