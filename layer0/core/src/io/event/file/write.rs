use crate::{
    Errno,
    driver::route::kind,
    io::{event::entry::Entry, transfer::Len},
};

pub type Completion<'d> = Entry<'d, kind::Write, Result<Len, Errno>>;
