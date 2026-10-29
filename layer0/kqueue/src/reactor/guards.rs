use dope_sys::{
    Errno,
    engine::admit,
    io::{
        reactor::{
            Full,
            complete::{self, Vacancy},
            read::{self, Arm as _, Family},
            ready::{
                self, Driver,
                store::{self, Exec, State as _, submit},
            },
            retry,
        },
        request,
        socket::{Domain, option},
    },
};

use crate::{
    Command, Kqueue,
    command::{Lowered, Message},
    reactor::{Engine, Key, Slot, Slots},
    sockets::Handle,
};

/// Native state and its reserved completion cell, before host flight commitment.
pub struct Guard<'a, S, V> {
    pub(crate) state: &'a mut S,
    pub(crate) vacant: V,
}

impl<'a, S, V> Guard<'a, S, V> {
    pub(in crate::reactor) fn new(state: &'a mut S, vacant: V) -> Self {
        const {
            assert!(
                std::mem::size_of::<Self>()
                    == std::mem::size_of::<&mut S>() + std::mem::size_of::<V>()
            );
        }
        Self { state, vacant }
    }
}

impl<'a, C, V> admit::Guard<C> for Guard<'a, C::State, V>
where
    C: submit::Context<Handle = Handle> + admit::Context<Native = Kqueue, Key = Key<C>>,
    C::Context: 'a,
    V: Vacancy<Event = <C::Context as Exec>::Event>,
    C::Inputs: request::Context<Target = Slot<C>, Outbound = C::Outbound>,
{
    fn submit(
        self,
        request: Command<Lowered<Slot<C>, C::Outbound, Domain, Message<Slot<C>>>>,
        key: Key<C>,
    ) {
        submit::Execute::<C>::execute(request, self.state, key, self.vacant);
    }
}

impl<C, V> admit::Options<C> for Guard<'_, C::State, V>
where
    C: ready::Driver + admit::Context<Native = Kqueue>,
    C::Inputs: request::Context<Target = Slot<C>>,
{
    fn set<O: option::Cursor>(&self, target: Slot<C>, options: Option<O>) -> Result<(), Errno> {
        self.state.set(target, options)
    }
}

type Context<C> = <C as Driver>::Context;
type Poll<C> = <Context<C> as store::Context>::Poll;
type Reader<C> = <Context<C> as Exec>::Reader;
type Writer<C> = <Context<C> as Exec>::Writer;
type Event<C> = <Context<C> as Exec>::Event;
type Arm<'a, C> = <Context<C> as Exec>::Arm<'a>;
type Write<'a, C> = <Context<C> as Exec>::Retry<'a>;
type ReadPlan<'a, C> = <Arm<'a, C> as read::Arm<'a, Reader<C>, Event<C>, Poll<C>>>::Prepared;
type WritePlan<'a, C> =
    <Write<'a, C> as retry::raw::Write<'a, Writer<C>, Event<C>, Poll<C>>>::Prepared;
type Failure<C> = <Poll<C> as ready::Poll<Slots<C>>>::Failure;
type Vacant<'a, C> = <<C as ready::Driver>::Queue as complete::Queue>::Vacancy<'a>;

/// Preparation retains the native operation and only the completion cell it needs.
pub enum Pending<'a, C: ready::Driver + 'a, const FAMILY: u8> {
    Read {
        prepared: ReadPlan<'a, C>,
        vacant: Vacant<'a, C>,
    },
    Write {
        prepared: WritePlan<'a, C>,
        vacant: Vacant<'a, C>,
    },
    Queued,
    Completed,
}

pub struct Immediate(admit::Cancelled);

impl<C: ready::Driver, const FAMILY: u8> admit::Submission for Pending<'_, C, FAMILY> {
    fn submit(self) -> admit::Cancelled {
        use complete::Cancellation as _;
        match self {
            Self::Read { prepared, vacant } => {
                let admit::Submitted = prepared.commit(vacant);
                admit::Cancelled::Submitted
            }
            Self::Write { prepared, vacant } => {
                let admit::Submitted = prepared.commit(vacant);
                admit::Cancelled::Submitted
            }
            Self::Queued => admit::Cancelled::Queued,
            Self::Completed => admit::Cancelled::Completed,
        }
    }
}

impl admit::Submission for Immediate {
    fn submit(self) -> admit::Cancelled {
        self.0
    }
}

impl<'a, C, const FAMILY: u8> admit::Reserve<'a, C, Engine<C>> for Pending<'a, C, FAMILY>
where
    C: submit::Context<Handle = Handle> + admit::Context<Native = Kqueue, Key = Key<C>>,
    C::Failure: From<Failure<C>>,
    Engine<C>: 'a,
    C::Inputs: request::Context<Target = Slot<C>, Outbound = C::Outbound>,
{
    fn try_new(
        backend: &'a mut Engine<C>,
        key: C::Key,
        pending: bool,
    ) -> Result<Self, admit::Error<admit::Cause<C::Failure>>> {
        use complete::Queue as _;
        if backend.pending.is_terminal(key) {
            return Ok(Self::Queued);
        }
        if !pending {
            return Ok(Self::Completed);
        }
        const { assert!(FAMILY < 3) };
        if FAMILY == 2 {
            let prepared = retry::raw::Write::prepare(backend.state.write(), key)
                .map_err(|cause| admit::Error::Rejected(admit::Cause::Reactor(cause.into())))?
                .ok_or(admit::Error::Rejected(admit::Cause::Absent(admit::Absent)))?;
            let vacant = backend.pending.vacant().ok_or(admit::Error::Full(Full))?;
            return Ok(Self::Write { prepared, vacant });
        }
        let family = if FAMILY == 0 {
            Family::Accept
        } else {
            Family::Recv
        };
        let prepared = backend
            .state
            .read()
            .prepare(key, family)
            .map_err(|cause| admit::Error::Rejected(admit::Cause::Reactor(cause.into())))?
            .ok_or(admit::Error::Rejected(admit::Cause::Absent(admit::Absent)))?;
        let vacant = backend.pending.vacant().ok_or(admit::Error::Full(Full))?;
        Ok(Self::Read { prepared, vacant })
    }
}

impl<'a, C> admit::Reserve<'a, C, Engine<C>> for Immediate
where
    C: ready::Driver + admit::Context<Native = Kqueue, Key = Key<C>>,
    Engine<C>: 'a,
{
    fn try_new(
        backend: &'a mut Engine<C>,
        key: C::Key,
        pending: bool,
    ) -> Result<Self, admit::Error<admit::Cause<C::Failure>>> {
        use complete::Queue as _;
        if backend.pending.is_terminal(key) {
            Ok(Self(admit::Cancelled::Queued))
        } else if pending {
            Err(admit::Error::Rejected(admit::Cause::Absent(admit::Absent)))
        } else {
            Ok(Self(admit::Cancelled::Completed))
        }
    }
}
