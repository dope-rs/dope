use crate::{
    Errno,
    driver::route::{self, kind},
    io::{self, event::{self, entry::Entry}, transfer::Len},
};

pub type Completion<'d, S = route::Route> = Entry<'d, kind::Send, Result<Len, Errno>, S>;

impl<'d, S: route::Selection> From<Completion<'d, S>> for io::Event<'d> {
    fn from(done: Completion<'d, S>) -> Self {
        Self(event::Kind::Send(done.erase_route()))
    }
}
