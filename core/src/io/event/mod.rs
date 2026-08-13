use crate::{driver::route, io};

mod targeted;

pub(super) use targeted::Targeted;

pub mod accept;
pub mod connect;
pub mod creation;
pub mod open;
pub mod receiving;
pub mod send;
pub mod tuning;

pub enum Kind<'d> {
    Accept(route::Token, accept::Completion<'d>),
    Recv(receiving::Completion<'d>),
    Send(send::Completion),
    Socket(creation::Completion<'d>),
    Tuning(tuning::Completion),
    Connect(connect::Completion),
    Open(open::Completion),
    Read(route::Token, io::ReadEvent),
    Write(route::Token, io::WriteEvent),
    Stat(route::Token, io::StatEvent),
    Sync(route::Token, io::Sync),
    Shutdown,
}
