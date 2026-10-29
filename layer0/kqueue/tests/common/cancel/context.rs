use std::num::NonZeroUsize;

use dope_sys::{
    Errno,
    io::reactor::{read, retry},
    memory,
};

use crate::common::{cancel::Extent, slots::Slots};

#[derive(Clone, Copy)]
pub struct Context;

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
    type Extent = Extent;
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

type Length = <Extent as memory::raw::Extent>::Length;
