use std::mem;

use dope_core::driver::route::Identity;
use response::{Item, Settlement, work};

use crate::client::{
    app::Wake,
    session::{self, response, turns},
};

#[derive(Clone, Copy)]
pub enum Entry {
    Resume,
    Chunk,
    Retire,
    Paused,
}

pub enum Start {
    Ready,
    Eof,
    Idle,
    Blocked,
    Yield,
    Close,
    Contract,
    Finishing,
    Finished,
}

#[derive(Clone, Copy)]
pub enum End {
    Continue,
    Close,
}

pub enum Record {
    Done,
    Ready,
    Blocked,
}

/// Response-flow state consumed by the session parser and wake path.
pub trait Policy: Default {
    fn record(&mut self, eof: bool, record: Record);
    fn wake(&mut self) -> Wake;
    fn reset(&mut self);
}

/// Starts one response step in the state that owns its continuation.
pub trait Begin<'d, ID: Identity, N, R>: Policy
where
    N: session::Session<'d, ID>,
    R: response::Plan,
{
    fn begin<'e, A: work::Admission>(
        &mut self,
        owner: &mut N,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, N::ConnState, N::Send, ID>,
        entry: Entry,
    ) -> Start;
}

/// Payload-free response continuation state.
#[derive(Clone, Copy, Default)]
pub enum State {
    #[default]
    Idle,
    InputReady,
    InputBlocked,
    EofInputReady,
    EofInputBlocked,
    EofFinished,
}

impl<'d, ID: Identity, N, R> Begin<'d, ID, N, R> for State
where
    N: session::Session<'d, ID>,
    R: response::Plan,
{
    fn begin<'e, A: work::Admission>(
        &mut self,
        _: &mut N,
        _: &mut A,
        _: &mut turns::Context<'_, 'e, 'd, N::ConnState, N::Send, ID>,
        entry: Entry,
    ) -> Start {
        match (entry, *self) {
            (Entry::Resume | Entry::Chunk | Entry::Retire, Self::Idle | Self::InputReady)
            | (Entry::Retire, Self::InputBlocked) => {
                *self = Self::Idle;
                Start::Ready
            }
            (Entry::Resume, Self::EofInputReady | Self::EofFinished) => {
                *self = Self::Idle;
                Start::Idle
            }
            (Entry::Resume, Self::InputBlocked | Self::EofInputBlocked) => Start::Blocked,
            (Entry::Chunk, _) => Start::Contract,
            (Entry::Paused, Self::Idle | Self::InputReady | Self::InputBlocked) => {
                *self = Self::Idle;
                Start::Idle
            }
            (Entry::Retire | Entry::Paused, Self::EofInputReady) => {
                *self = Self::EofFinished;
                Start::Finishing
            }
            (Entry::Retire | Entry::Paused, Self::EofFinished) => Start::Finished,
            (Entry::Retire | Entry::Paused, Self::EofInputBlocked) => Start::Blocked,
        }
    }
}

impl Policy for State {
    fn record(&mut self, eof: bool, record: Record) {
        *self = match (eof, record) {
            (false, Record::Done) => Self::Idle,
            (false, Record::Ready) => Self::InputReady,
            (false, Record::Blocked) => Self::InputBlocked,
            (true, Record::Done) => Self::EofFinished,
            (true, Record::Ready) => Self::EofInputReady,
            (true, Record::Blocked) => Self::EofInputBlocked,
        };
    }

    fn wake(&mut self) -> Wake {
        match self {
            Self::InputBlocked => {
                *self = Self::InputReady;
                Wake::Ready
            }
            Self::EofInputBlocked => {
                *self = Self::EofInputReady;
                Wake::Ready
            }
            _ => Wake::Idle,
        }
    }

    fn reset(&mut self) {
        *self = Self::Idle;
    }
}

/// Response continuation state that owns resumable application work.
#[derive(Default)]
pub enum Pending<P> {
    #[default]
    Idle,
    InputReady,
    InputBlocked,
    Ready(P),
    Blocked(P),
    CloseReady(P),
    CloseBlocked(P),
    EofInputReady,
    EofInputBlocked,
    EofFinished,
    EofReady(P),
    EofBlocked(P),
}

const _: () = {
    assert!(mem::size_of::<State>() == 1);
    assert!(mem::size_of::<Pending<()>>() == 1);
};

impl<P> Pending<Item<P>> {
    fn settle<'e, 'd, ID: Identity, N, A, R>(
        &mut self,
        owner: &mut N,
        item: Item<P>,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, N::ConnState, N::Send, ID>,
        eof: bool,
        end: End,
    ) -> Start
    where
        N: session::Resume<'d, R, P, ID>,
        A: work::Admission,
    {
        match owner.settle(item, work, ctx) {
            Settlement::Ready => match (eof, end) {
                (true, End::Close) | (false, End::Close) => Start::Close,
                (true, End::Continue) => Start::Eof,
                (false, End::Continue) => Start::Ready,
            },
            Settlement::Yield(item) => {
                self.ready(eof, end, Item::new(item));
                Start::Yield
            }
            Settlement::Blocked(item) => {
                self.block(eof, end, Item::new(item));
                Start::Blocked
            }
        }
    }

    pub(super) fn ready(&mut self, eof: bool, end: End, item: Item<P>) {
        *self = match (eof, end) {
            (true, End::Close) | (false, End::Close) => Self::CloseReady(item),
            (true, End::Continue) => Self::EofReady(item),
            (false, End::Continue) => Self::Ready(item),
        };
    }

    pub(super) fn block(&mut self, eof: bool, end: End, item: Item<P>) {
        *self = match (eof, end) {
            (true, End::Close) | (false, End::Close) => Self::CloseBlocked(item),
            (true, End::Continue) => Self::EofBlocked(item),
            (false, End::Continue) => Self::Blocked(item),
        };
    }
}

impl<'d, ID: Identity, N, R, P, Q> Begin<'d, ID, N, R> for Pending<Item<Q>>
where
    R: response::Plan<Prepared = P, Pending = Q>,
    N: session::Resume<'d, P, Q, ID>,
{
    fn begin<'e, A: work::Admission>(
        &mut self,
        owner: &mut N,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, N::ConnState, N::Send, ID>,
        entry: Entry,
    ) -> Start {
        match (entry, mem::replace(self, Self::Idle)) {
            (Entry::Chunk, Self::Idle | Self::InputReady) => Start::Ready,
            (Entry::Chunk, state) => {
                *self = state;
                Start::Contract
            }
            (Entry::Resume, Self::Idle | Self::InputReady) => Start::Ready,
            (Entry::Resume, Self::EofInputReady | Self::EofFinished) => Start::Idle,
            (Entry::Retire, Self::Idle | Self::InputReady | Self::InputBlocked) => Start::Ready,
            (Entry::Paused, Self::Idle | Self::InputReady | Self::InputBlocked) => Start::Idle,
            (Entry::Resume | Entry::Retire | Entry::Paused, Self::Ready(item)) => {
                self.settle::<ID, N, A, P>(owner, item, work, ctx, false, End::Continue)
            }
            (Entry::Resume | Entry::Retire | Entry::Paused, Self::EofReady(item)) => {
                self.settle::<ID, N, A, P>(owner, item, work, ctx, true, End::Continue)
            }
            (Entry::Resume | Entry::Retire | Entry::Paused, Self::CloseReady(item)) => {
                self.settle::<ID, N, A, P>(owner, item, work, ctx, false, End::Close)
            }
            (Entry::Retire | Entry::Paused, Self::EofInputReady) => {
                *self = Self::EofFinished;
                Start::Finishing
            }
            (Entry::Retire | Entry::Paused, Self::EofFinished) => {
                *self = Self::EofFinished;
                Start::Finished
            }
            (
                Entry::Resume | Entry::Retire | Entry::Paused,
                state @ (Self::Blocked(_)
                | Self::CloseBlocked(_)
                | Self::EofBlocked(_)
                | Self::EofInputBlocked),
            )
            | (Entry::Resume, state @ Self::InputBlocked) => {
                *self = state;
                Start::Blocked
            }
        }
    }
}

impl<P> Policy for Pending<P> {
    fn record(&mut self, eof: bool, record: Record) {
        *self = match (eof, record) {
            (false, Record::Done) => Self::Idle,
            (false, Record::Ready) => Self::InputReady,
            (false, Record::Blocked) => Self::InputBlocked,
            (true, Record::Done) => Self::EofFinished,
            (true, Record::Ready) => Self::EofInputReady,
            (true, Record::Blocked) => Self::EofInputBlocked,
        };
    }

    fn wake(&mut self) -> Wake {
        match mem::replace(self, Self::Idle) {
            Self::InputBlocked => {
                *self = Self::InputReady;
                Wake::Ready
            }
            Self::Blocked(item) => {
                *self = Self::Ready(item);
                Wake::Ready
            }
            Self::CloseBlocked(item) => {
                *self = Self::CloseReady(item);
                Wake::Ready
            }
            Self::EofInputBlocked => {
                *self = Self::EofInputReady;
                Wake::Ready
            }
            Self::EofBlocked(item) => {
                *self = Self::EofReady(item);
                Wake::Ready
            }
            state => {
                *self = state;
                Wake::Idle
            }
        }
    }

    fn reset(&mut self) {
        *self = Self::Idle;
    }
}
