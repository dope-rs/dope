use dope_net::link::{
    egress::{self, load, reserve::exact},
    event,
    pool::change::opening,
};
use o3::buffer::storage::Shared;

use crate::common::manifold::client::{
    connect, finish, kind, probe, send, side, switch, switch::decode,
};

pub enum Error {
    Connect(event::ConnectFault),
    Exhaust(opening::Exhaustion),
    Open(opening::Exhaustion),
    Receive(dope_core::Errno),
    Enqueue(egress::Rejected<load::Value<Shared>>),
    Admit(exact::Unavailable<Shared>),
    Decode(decode::Error),
    Send(send::Error),
    Finish(finish::Error),
    Probe(probe::Error),
    Connection(connect::Error),
    Switch(switch::Error),
    Kind(kind::Error),
    Side(side::Error),
}

#[derive(Debug)]
pub enum Cause<'a> {
    Connect(&'a event::ConnectFault),
    Exhaust(&'a opening::Exhaustion),
    Open(&'a opening::Exhaustion),
    Receive(&'a dope_core::Errno),
    Enqueue(&'a egress::Rejection),
    Admit(exact::Failure),
    Decode(&'a decode::Error),
    Send(&'a send::Error),
    Finish(&'a finish::Error),
    Probe(&'a probe::Error),
    Connection(&'a connect::Error),
    Switch(&'a switch::Error),
    Kind(&'a kind::Error),
    Side(&'a side::Error),
}

impl Error {
    pub fn check(errors: &[Self]) -> Result<(), Cause<'_>> {
        match errors.first() {
            None => Ok(()),
            Some(Self::Connect(cause)) => Err(Cause::Connect(cause)),
            Some(Self::Exhaust(cause)) => Err(Cause::Exhaust(cause)),
            Some(Self::Open(cause)) => Err(Cause::Open(cause)),
            Some(Self::Receive(cause)) => Err(Cause::Receive(cause)),
            Some(Self::Enqueue(rejected)) => Err(Cause::Enqueue(rejected.error())),
            Some(Self::Admit(rejected)) => Err(Cause::Admit(rejected.cause())),
            Some(Self::Decode(cause)) => Err(Cause::Decode(cause)),
            Some(Self::Send(cause)) => Err(Cause::Send(cause)),
            Some(Self::Finish(cause)) => Err(Cause::Finish(cause)),
            Some(Self::Probe(cause)) => Err(Cause::Probe(cause)),
            Some(Self::Connection(cause)) => Err(Cause::Connection(cause)),
            Some(Self::Switch(cause)) => Err(Cause::Switch(cause)),
            Some(Self::Kind(cause)) => Err(Cause::Kind(cause)),
            Some(Self::Side(cause)) => Err(Cause::Side(cause)),
        }
    }
}

impl From<connect::Error> for Error {
    fn from(cause: connect::Error) -> Self {
        Self::Connection(cause)
    }
}
