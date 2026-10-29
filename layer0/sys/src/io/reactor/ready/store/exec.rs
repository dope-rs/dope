use crate::{
    io::reactor::{
        read,
        ready::{self, store::Context},
        retry,
    },
    memory::buffer,
    table,
    table::registry,
};

/// Borrowed readiness operations over the exact stored owners.
pub trait Exec: Context<Read: registry::Entry<Echo = Self::Key>> {
    type Key: Copy + Eq;
    type Reader: read::Context<Slots = Self::Slots, Key = Self::Key>;
    type Writer: retry::Context<Slots = Self::Slots, Key = Self::Key>;
    type Event: read::Complete<
            Key = Self::Key,
            Flow = <Self::Reader as read::Context>::Flow,
            Accepted = <Self::Slots as table::raw::Slots>::Accepted,
            Buffer = <Self::Pool as buffer::Pool>::Buffer,
        > + retry::Complete<Self::Writer>;
    type Send: retry::raw::Send<Self::Writer>;
    type Arm<'a>: read::Arm<
            'a,
            Self::Reader,
            Self::Event,
            Self::Poll,
            Files = Self::Files,
            Entry = Self::Read,
            Registry = Self::Reads,
            Failure = <Self::Poll as ready::Poll<Self::Slots>>::Failure,
        >
    where
        Self: 'a;
    type Retry<'a>: retry::raw::Write<
            'a,
            Self::Writer,
            Self::Event,
            Self::Poll,
            Files = Self::Files,
            Entry = Self::Pending,
            State = Self::Writes,
            Send = Self::Send,
            Failure = <Self::Poll as ready::Poll<Self::Slots>>::Failure,
        >
    where
        Self: 'a;
    type ReadPoll<'a>: read::raw::Poll<
            'a,
            Self::Reader,
            <Self::Pool as buffer::Pool>::Layout,
            Self::Event,
            Self::Poll,
            Arm = Self::Arm<'a>,
            Pool = Self::Pool,
        >
    where
        Self: 'a;
}
