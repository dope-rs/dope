use o3::buffer::resident;

use crate::net::error::{self, read::Read};

pub(crate) enum Recv<R, E> {
    Ready(R),
    Closed,
    Failed(Read<E>),
    Pending,
}

pub(crate) enum SendStatus<'d> {
    Complete,
    Failed(error::Write<'d>),
    Pending,
}

pub(crate) enum StageSend<'d> {
    Staged,
    Busy(resident::Shared<'d>),
    Failed(error::Write<'d>),
}
