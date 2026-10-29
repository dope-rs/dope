use std::{convert::Infallible, num::NonZeroUsize};

use dope_sys::{
    Errno,
    io::{
        reactor::{complete, read},
        socket::accept,
    },
};

use crate::common::cancel::buffer::Buffer;

#[derive(Debug)]
pub struct Event {
    pub key: NonZeroUsize,
    pub result: Result<(), accept::Error>,
    pub more: bool,
}

impl complete::Event for Event {
    type Key = NonZeroUsize;
    fn token(&self) -> Option<NonZeroUsize> {
        Some(self.key)
    }
    fn is_terminal(&self) -> bool {
        !self.more
    }
}

impl read::Complete for Event {
    type Accepted = Infallible;
    type Buffer = Buffer;
    type Flow = bool;

    const LAST: bool = false;
    const MORE: bool = true;

    fn accepted(_: NonZeroUsize, accepted: Infallible, _: bool) -> Self {
        match accepted {}
    }
    fn received(_: NonZeroUsize, buffer: Buffer, _: bool) -> Self {
        match buffer {}
    }
    fn rejected(key: NonZeroUsize, cause: accept::Error, more: bool) -> Self {
        Self {
            key,
            result: Err(cause),
            more,
        }
    }
    fn control(key: NonZeroUsize, result: Result<(), Errno>, more: bool) -> Self {
        Self {
            key,
            result: result.map_err(accept::Error::Syscall),
            more,
        }
    }
}

impl Event {
    pub fn try_emit(event: complete::Dequeued<Self>) -> Result<Self, Self> {
        match event {
            complete::Dequeued::Emit(event) => Ok(event),
            complete::Dequeued::Reclaim(event) => Err(event),
        }
    }
}
