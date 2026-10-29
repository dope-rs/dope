mod uring;
use std::{convert::Infallible, mem, num};

use dope_sys::{
    Errno, engine,
    engine::{
        admit,
        ring::{Context, config, config::Config, create::Failure},
    },
    io::{
        file, pipe,
        reactor::{complete, complete::Observe, ready::delivery, submit},
        socket::{
            self, accept, gso, option,
            option::{fast, tuning, tuning::device::Source},
            record,
            route::{self, install},
        },
    },
    kernel,
    kernel::errno,
    memory,
    memory::buffer::{self, provided},
    probe,
    probe::{accept::Validate, cleanup::Rejection, receive, session},
    ring,
    ring::{create, enter, map, prepare, ready},
    table::{
        permit,
        token::{self, Codec, host},
    },
    thread::affinity,
};
use driver::Engine;
use o3::{base::num::bounded, buffer::bytes::bounded::MAX};
pub(crate) use observed::Observed;
use reactor::{
    buffers,
    ring::{Recover, Released, Resources},
    sq,
};
pub(crate) use reactor::{register::Register, ring::Ring};
pub(crate) use sockets::Opened;
pub use uring::Uring;

use crate::{
    driver::{
        Closing, Turn,
        poll::Poll,
        token::Encoded,
        tuning::{State, Table},
    },
    reactor::{
        buffers::{Canary, Cleanup, Entry, Payload, Producer, Selected},
        cq,
        cq::packet,
        op,
        ring::{Accepted, Enter, Outcome, Range, Ready, Session, Waiting},
        sq::Borrowed,
        types,
    },
    sockets::{Control, Installer, Operation},
    system::{Cpus, Fifo, Timer},
};

mod reactor;

mod driver;

mod error;
mod observed;
mod system;

mod sockets;

type BufferBuild<L, P> = buffers::Build<L, P>;

type Primary = create::Primary<
    create::Cause<kernel::Errno<error::Setup>, kernel::Errno<error::Map>>,
    kernel::Errno<error::Register<20>>,
    kernel::Errno<error::Register<22>>,
>;
type Fault<T> = create::Failure<
    map::Storage<kernel::Errno<error::Unmap>>,
    probe::Cause<
        <<T as Codec>::Context as host::Context>::Probe,
        Observed<T>,
        kernel::Errno<error::Enter>,
    >,
>;

type Mapping = map::Map<kernel::Errno<error::Map>>;
type Unmap = map::Failure<kernel::Errno<error::Unmap>>;

type Admission = prepare::Rejection<
    kernel::Errno<error::Register<24>>,
    kernel::Errno<error::Register<13>>,
    kernel::Errno<error::Register<25>>,
>;

type Host<C> = <<C as Context>::Token as token::Codec>::Context;
type Update = kernel::Errno<error::Register<6>>;
type Cause<C> = <Host<C> as probe::Build<Update>>::Primary;
type Removal<C> = <Host<C> as probe::Build<Update>>::Removed;
type Retained<C> = Resources<<C as Context>::Token, <C as Context>::Layout, Cause<C>>;
type Recovery<C> =
    Result<Cause<C>, Recover<<C as Context>::Token, <C as Context>::Layout, Cause<C>>>;
type Removed<C> = Result<
    (Removal<C>, sq::Fixed),
    Released<<C as Context>::Token, <C as Context>::Layout, Removal<C>>,
>;
type Created<C> = Failure<
    <Ring<<C as Context>::Token> as create::Create>::Setup,
    <Ring<<C as Context>::Token> as create::Create>::Map,
    Cause<C>,
    Retained<C>,
    Removed<C>,
>;

type Built<C, G> = (
    Engine<C>,
    buffers::raw::Context<<C as Context>::Layout>,
    <Uring as permit::Source>::Queue<<C as Config<G>>::Key, <C as Config<G>>::Index>,
);

mod raw;
