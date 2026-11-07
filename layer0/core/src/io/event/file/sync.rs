use crate::{Errno, driver::route::kind, io::event::entry::Entry};

pub type Completion<'d> = Entry<'d, kind::Sync, Result<(), Errno>>;
