pub mod batch;
mod entry;

pub mod file;
pub mod net;

use file::{open, sync, write};
use net::{accept, connect, recv, send, socket, tuning};

pub enum Kind<'d> {
    Accept(accept::Completion<'d>),
    Recv(recv::Completion<'d>),
    RetiredRecv(recv::Completion<'d>),
    Send(send::Completion<'d>),
    Socket(socket::Completion<'d>),
    Tuning(tuning::Completion<'d>),
    Connect(connect::Completion<'d>),
    Open(open::Completion<'d>),
    Write(write::Completion<'d>),
    Sync(sync::Completion<'d>),
    Shutdown,
}
