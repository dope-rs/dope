use std::num::{NonZeroU32, NonZeroUsize};

use dope_sys::{
    Errno,
    io::reactor::{read, retry},
    memory,
};
use o3::{base::num::bounded, buffer};

use crate::common::slots::Slots;

#[derive(Clone, Copy)]
pub enum Context {}

impl read::Context for Context {
    type Slots = Slots;
    type Key = NonZeroUsize;
    type Flow = bool;
    type Operation = ();

    const ACCEPT: () = ();
    const RECV: () = ();
    const MESSAGE: () = ();
}

impl retry::Context for Context {
    type Slots = Slots;
    type Key = NonZeroUsize;
    type Operation = ();
    type Extent = Self;
    type Outcome = Result<Length, Errno>;
    type Unit = Result<(), Errno>;

    const SOCKET_ERROR: () = ();
    const SEND: () = ();
    const CONNECT: () = ();

    fn sent(result: Result<Length, Errno>) -> Self::Outcome {
        result
    }
    fn connected(result: Result<(), Errno>) -> Self::Unit {
        result
    }
}

type Length = bounded::U32<0, { buffer::bytes::bounded::MAX as u32 }>;

// SAFETY: Length retains the original admitted count and Capacity is unchanged.
unsafe impl memory::raw::Extent for Context {
    type Length = Length;
    type Capacity = NonZeroU32;
}

impl dope_sys::Buffer for Context {
    fn output(&mut self) -> buffer::raw::Output<'_> {
        match *self {}
    }
    fn len(&self) -> usize {
        match *self {}
    }
    fn recycle(self) {
        match self {}
    }
}
