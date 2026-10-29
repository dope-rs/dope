use std::marker;

use dope_sys::io::{
    reactor::{
        read::Source,
        ready,
        ready::{
            delivery::{Decoded, entry, entry::Event as _},
            store,
        },
        retry,
    },
    socket::Prepare,
};

use crate::driver::{
    life::native::host::{
        Data, Done, Files, Pending, Pool, ReadSlot, Reader, ReadyPoll, Receive, Registry, Retry,
        Writes, poll::files,
    },
    retained::flight,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Context<N>(marker::PhantomData<N>);

impl<N: ready::Source + Source + retry::Source> store::Context for Context<N> {
    type Slots = files::Slots;
    type Files = Files<N>;
    type Read = ReadSlot<N>;
    type Reads = Registry<N>;
    type Pending = Pending<N>;
    type Writes = Writes<N>;
    type Poll = ReadyPoll<N>;
    type Pool = Pool<N>;
}

impl<N> store::Exec for Context<N>
where
    N: ready::Source + Source + retry::Source + entry::Source + Prepare,
{
    type Key = flight::raw::Echo;
    type Reader = files::Slots;
    type Writer = files::Slots;
    type Event = Done<N>;
    type Send = Data<N>;
    type Arm<'a>
        = Reader<'a, N>
    where
        Self: 'a;
    type Retry<'a>
        = Retry<'a, N>
    where
        Self: 'a;
    type ReadPoll<'a>
        = <N as Source>::Poll<'a, files::Slots, Receive, Done<N>, ReadyPoll<N>>
    where
        Self: 'a;
}

impl<N> ready::raw::Decode for Context<N>
where
    N: ready::Source + Source + retry::Source + entry::Source + Prepare,
{
    unsafe fn decode(raw: usize) -> Decoded<flight::raw::Echo> {
        if raw == usize::MAX {
            return Decoded::Shutdown;
        }
        // SAFETY: Decode receives user data from this context's native reactor;
        // the shutdown sentinel has been removed before flight decoding.
        match unsafe { flight::raw::Echo::from_kernel(raw as u64) } {
            Some(key) => Decoded::Public(key),
            None => Decoded::Empty,
        }
    }

    const SHUTDOWN: Done<N> = Done::<N>::SHUTDOWN;
}
