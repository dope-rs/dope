use std::net;

use dope_sys::{
    io::reactor::complete::message::{Cut, Output as _},
    probe::receive::{self, Corrupt, Failure},
};

use crate::reactor::cq::{Capacity, Length, Output};

pub struct Payload;

impl receive::Receive for Payload {
    type Capacity = Capacity;
    type Length = Length;

    fn bytes(actual: &[u8], want: &[u8]) -> Result<(), Corrupt> {
        if actual == want { Ok(()) } else { Err(Corrupt) }
    }

    fn message<const NAME: usize, E>(
        bytes: &[u8],
        want: &[u8],
        parse: impl FnOnce(&[u8]) -> Result<net::SocketAddr, E>,
    ) -> Result<(), Failure<Self::Capacity, Self::Length, E>> {
        let output = Output::<NAME, 0>::parse(bytes).map_err(Failure::Frame)?;
        match (output.payload_cut(), output.name_cut()) {
            (Cut::Complete, Cut::Complete) => {}
            (Cut::Complete, Cut::Truncated)
            | (Cut::Truncated, Cut::Complete)
            | (Cut::Truncated, Cut::Truncated) => return Err(Failure::Truncated),
        }
        let source = parse(output.name()).map_err(Failure::Address)?;
        if !source.ip().is_loopback() {
            return Err(Failure::Payload(Corrupt));
        }
        Self::bytes(output.payload(), want).map_err(Failure::Payload)
    }
}
