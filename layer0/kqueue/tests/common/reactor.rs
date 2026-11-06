use std::{convert::Infallible, num::NonZero, task};

use dope_kqueue::Kqueue;
use dope_sys::{
    engine,
    io::{
        reactor::{
            complete, read,
            ready::{self, delivery, delivery::entry, store},
            retry,
        },
        socket,
    },
    memory::{self, buffer},
};
use o3::mem::quota;

use crate::common::{
    Budget, Failure, Files, Poll,
    cancel::Context,
    slots::{Slots, inputs::Physical},
};

pub struct Host;
pub type Engine = <Kqueue as engine::Source<Host>>::Backend;
pub type Pool = <Kqueue as buffer::Source>::Pool<Receive>;
pub type Buffer = <Pool as buffer::Pool>::Buffer;
pub type Event = <Kqueue as entry::Source>::Event<Host, Buffer>;
pub type Packet = complete::Dequeued<Event>;
pub type Read = <Kqueue as read::Source>::Entry<Context>;
type Pending = <Kqueue as retry::Source>::Pending<Context>;
pub type Turn<'a> = <Kqueue as engine::Dispatch<'a, Host, Engine>>::Turn<&'a Quota>;

#[derive(Clone, Copy)]
pub struct Receive;

// SAFETY: two eight-byte slots satisfy the native count, capacity, and product bounds.
unsafe impl memory::raw::Layout for Receive {
    fn entries(self) -> NonZero<u16> {
        NonZero::<u16>::MIN.saturating_add(1)
    }

    fn capacity(self) -> NonZero<u32> {
        NonZero::<u32>::MIN.saturating_add(7)
    }
}

impl entry::Context for Host {
    type Reader = Context;
    type Writer = Context;
    type Outbound = Physical;
    type Handle = <Kqueue as socket::Prepare>::Handle;
    type Native = Kqueue;
    type Failure = socket::accept::Error;
    type Reclaimed = ();

    const LAST: bool = false;
    const MORE: bool = true;
}

impl store::Context for Host {
    type Slots = Slots;
    type Files = Files;
    type Read = Read;
    type Reads = <Kqueue as read::Source>::Registry<Context>;
    type Pending = Pending;
    type Writes = <Kqueue as retry::Source>::State<Pending>;
    type Poll = Poll;
    type Pool = Pool;
}

impl store::Exec for Host {
    type Key = NonZero<usize>;
    type Reader = Context;
    type Writer = Context;
    type Event = Event;
    type Send = <Kqueue as retry::Source>::Send<Context>;
    type Arm<'a> = <Kqueue as read::Source>::Arm<'a, Context, Event, Poll>;
    type Retry<'a> = <Kqueue as retry::Source>::Retry<'a, Context, Event, Poll>;
    type ReadPoll<'a> = <Kqueue as read::Source>::Poll<'a, Context, Receive, Event, Poll>;
}

impl ready::raw::Decode for Host {
    unsafe fn decode(raw: usize) -> delivery::Decoded<NonZero<usize>> {
        if raw == usize::MAX {
            delivery::Decoded::Shutdown(dope_sys::engine::register::Interest::Readable)
        } else if raw == usize::MAX - 1 {
            delivery::Decoded::Shutdown(dope_sys::engine::register::Interest::Closed)
        } else {
            match NonZero::new(raw) {
                Some(key) => delivery::Decoded::Public(key),
                None => delivery::Decoded::Empty,
            }
        }
    }

    const SHUTDOWN: Event = <Event as entry::Event<Self, Buffer>>::SHUTDOWN;
}

impl ready::Driver for Host {
    type Map = Event;
    type Observation = ();
    type Context = Self;
    type State = <Kqueue as ready::Source>::State<Self>;
    type Queue = <Kqueue as complete::Source>::Queue<Event, NonZero<u32>>;

    fn shutdown(interest: dope_sys::engine::register::Interest) -> NonZero<usize> {
        match interest {
            dope_sys::engine::register::Interest::Readable => NonZero::<usize>::MAX,
            dope_sys::engine::register::Interest::Closed => NonZero::new(usize::MAX - 1).unwrap(),
        }
    }
}

pub struct Dispatched(pub Result<(task::Poll<()>, Option<Packet>), Failure>);

impl From<(task::Poll<()>, Option<Packet>)> for Dispatched {
    fn from((state, held): (task::Poll<()>, Option<Packet>)) -> Self {
        Self(Ok((state, held)))
    }
}

impl From<(task::Poll<()>, Infallible)> for Dispatched {
    fn from((_, never): (task::Poll<()>, Infallible)) -> Self {
        match never {}
    }
}

impl From<Failure> for Dispatched {
    fn from(error: Failure) -> Self {
        Self(Err(error))
    }
}

impl delivery::Context<Engine> for Host {
    type Backend = Engine;
    type Event = Packet;
    type Failure = Infallible;
    type Dispatch = Dispatched;

    fn backend(host: &mut Engine) -> &mut Engine {
        host
    }

    fn resolve(host: &mut Engine) -> Option<Result<Option<Packet>, Infallible>> {
        engine::Complete::next(host).map(Ok)
    }
}

pub struct Quota(pub quota::Ledger);

impl store::Quota for &Quota {
    type Budget<'a, Lane>
        = Budget<'a>
    where
        Self: 'a;

    fn remaining(&self) -> usize {
        self.0.remaining()
    }

    fn budget<Lane>(&self, limit: usize) -> Budget<'_> {
        Budget::limited(&self.0, limit)
    }

    fn refund(self) {}
}

impl delivery::Quota<Engine> for &Quota {
    fn reclaim(&self, _: &mut Engine) {}
}
