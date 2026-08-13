use std::io;

use dope::net::link::egress::data;

pub(crate) enum Recv<R> {
    Ready(R),
    Closed,
    Failed(io::ErrorKind),
    Pending,
}

pub(crate) enum SendStatus {
    Complete,
    Failed(io::ErrorKind),
    Pending,
}

pub(crate) enum StageSend<'d> {
    Staged,
    Busy(data::Buffer<'d>),
    Failed(io::ErrorKind),
}
