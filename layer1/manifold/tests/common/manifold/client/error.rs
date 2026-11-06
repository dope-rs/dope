use dope_manifold::client::attempt;
use dope_net::link::{
    egress::{self, reserve::exact},
    pool::change::opening,
};

use crate::common::manifold::client::{
    connect, finish, kind, probe, send, side, switch, switch::decode,
};

#[derive(Debug)]
pub enum Error {
    Connect(attempt::ConnectFault),
    Exhaust(opening::Exhaustion),
    Open(opening::Exhaustion),
    Receive(dope_core::Errno),
    Enqueue(egress::Rejection),
    Admit(exact::Failure),
    Decode(decode::Error),
    Send(send::Error),
    Finish(finish::Error),
    Probe(probe::Error),
    Connection(connect::Error),
    Switch(switch::Error),
    Kind(kind::Error),
    Side(side::Error),
}

impl From<connect::Error> for Error {
    fn from(cause: connect::Error) -> Self {
        Self::Connection(cause)
    }
}
