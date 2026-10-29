use std::{convert::Infallible, ops, os::fd, task, time};

use dope_sys::{
    Budget, engine,
    engine::{admit, register, shutdown},
    io::{
        Close,
        reactor::{
            Full, complete,
            complete::Queue as _,
            read,
            ready::{
                self, Backend as _, Failure as _, Poll as _,
                store::{Context, Create, Exec, Quota, State as _, submit},
            },
            retry,
            retry::State as _,
        },
        request,
    },
    memory::{buffer, buffer::Pool as _},
    table,
    table::{
        Files as _, change,
        change::{Changes as _, Delete as _},
        closed,
        reclaim::{Item, Kind, View},
        registry::State as _,
    },
};
use o3::{depot::map::Plan, mem::quota::Admission};

use crate::{
    Kqueue,
    reactor::{
        Guard, Key, Owned, Slot, Slots, Work,
        guards::{Immediate, Pending},
        state,
    },
    sockets::Handle,
};

pub struct Engine<C: ready::Driver> {
    pub(in crate::reactor) state: C::State,
    pub(in crate::reactor) pending: C::Queue,
}

impl<C: ready::Driver> Engine<C> {
    /// Current local accepted-slot availability; this does not reserve the slot.
    pub fn has_local_accept_capacity(&self) -> bool {
        self.state.view().files.has_accept_capacity()
    }
}

impl<C> engine::Complete for Engine<C>
where
    C: ready::Driver,
    C::Map: complete::Map<
            Entry<<<C::Context as Context>::Pool as buffer::Pool>::Buffer> = <C::Context as Exec>::Event,
        >,
{
    type Map = complete::Dequeued<C::Map>;
    type Pending = <<C::Context as Context>::Pool as buffer::Pool>::Buffer;

    fn next(&mut self) -> Option<Option<<Self::Map as complete::Map>::Entry<Self::Pending>>> {
        self.pending.pop_front().map(Some)
    }
}

impl<C: ready::Driver> engine::Buffer for Engine<C> {
    type Layout = <<C::Context as Context>::Pool as buffer::Pool>::Layout;
    type Token = <<C::Context as Context>::Pool as buffer::Pool>::Buffer;
    type Context = <<C::Context as Context>::Pool as buffer::Pool>::Context;
    type Selection = Infallible;
}

impl<C: ready::Driver> engine::Install<Owned<C>> for Engine<C> {
    type Handle = <<C::Context as Context>::Files as table::Files<Slots<C>>>::Handle;
    type Error = table::Install<<Slots<C> as change::raw::Slots>::Index>;

    fn install(&mut self, slot: Owned<C>, handle: Self::Handle) -> Result<(), Self::Error> {
        self.state.edit().files.install(slot, handle)
    }
}

impl<C: ready::Driver, S> engine::Cancel<S> for Engine<C> {
    type Permit = Infallible;

    fn cancel(&mut self, permit: &Self::Permit, _: S) -> Result<(), Full> {
        match *permit {}
    }
}

impl<'d, C, V> table::Retire<'d, V> for Engine<C>
where
    C: ready::Driver<Context: 'd>,
    V: View<Slot = <Self as ready::Backend<C>>::Slot>,
{
    type Output = <Self as ready::Backend<C>>::Closed;

    fn retire<'a>(
        &'a mut self,
        pending: Kind<V::Close, V::Retire>,
        view: V,
    ) -> task::Poll<Self::Output>
    where
        'd: 'a,
    {
        let slot = match &pending {
            Kind::Close(item) => item.slot(),
            Kind::Retire(item) => item.slot(),
        };
        let done = self.close(slot);
        view.commit(pending);
        task::Poll::Ready(done)
    }
}

impl<C> admit::Admit<C> for Engine<C>
where
    C: submit::Context<Handle = Handle> + admit::Context<Native = Kqueue, Key = Key<C>>,
    C::Inputs: request::Context<Target = Slot<C>, Outbound = C::Outbound>,
{
    type Admission<'a>
        = Guard<'a, C::State, <C::Queue as complete::Queue>::Vacancy<'a>>
    where
        Self: 'a;

    fn try_admit(&mut self) -> Result<Self::Admission<'_>, Full> {
        if self.state.view().poll.is_failed() {
            return Err(Full);
        }
        let Some(vacant) = self.pending.vacant() else {
            return Err(Full);
        };
        Ok(Guard::new(&mut self.state, vacant))
    }
}

impl<C> admit::Backend<C> for Engine<C>
where
    C: submit::Context<Handle = Handle> + admit::Context<Native = Kqueue, Key = Key<C>>,
    C::Inputs: request::Context<Target = Slot<C>, Outbound = C::Outbound>,
    C::Failure: From<<Self as ready::Backend<C>>::Failure>,
{
    type Pending<'a, const FAMILY: u8>
        = Pending<'a, C, FAMILY>
    where
        Self: 'a;
    type Immediate<'a>
        = Immediate
    where
        Self: 'a;
}

impl<C: ready::Driver> ready::Backend<C> for Engine<C> {
    type Failure =
        <<C::Context as Context>::Poll as ready::Poll<<C::Context as Context>::Slots>>::Failure;
    type Slot = <<C::Context as Context>::Slots as change::raw::Slots>::Physical;
    type Closed =
        closed::Completed<Self::Slot, <Slots<C> as table::raw::Slots>::Missing, Self::Failure>;
    fn try_new(
        recv: <<C::Context as Context>::Pool as buffer::Pool>::Layout,
        slots: <<C::Context as Context>::Slots as table::raw::Slots>::Layout,
        plan: Plan,
        capacity: <C::Queue as complete::Queue>::Capacity,
    ) -> Result<
        (
            Self,
            <<C::Context as Context>::Pool as buffer::Pool>::Context,
        ),
        Create,
    > {
        let (recv, context) =
            <C::Context as Context>::Pool::try_new(recv).map_err(Create::Buffer)?;
        let reads = <C::Context as Context>::Reads::try_new(plan).map_err(Create::Allocation)?;
        let retries = <C::Context as Context>::Writes::try_new(plan).map_err(Create::Allocation)?;
        let pending = C::Queue::try_new(capacity).map_err(Create::Queue)?;
        let files = <C::Context as Context>::Files::try_new(slots).map_err(Create::Allocation)?;
        let poll = <C::Context as Context>::Poll::try_new().map_err(Create::Reactor)?;
        Ok((
            Self {
                state: C::State::new(retries, files, poll, reads, recv),
                pending,
            },
            context,
        ))
    }

    fn state(&self) -> &C::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut C::State {
        &mut self.state
    }

    fn cursor(&mut self) -> usize {
        self.state.edit().poll.next_lane()
    }

    fn change_len(&self) -> usize {
        self.state.view().files.changes().len()
    }

    fn available(&self) -> usize {
        self.pending.available()
    }

    fn has_changes(&self) -> bool {
        !self.state.view().files.changes().is_empty()
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    fn can_resume(&self) -> bool {
        self.state.view().reads.can_resume()
    }

    fn check(&self) -> Result<(), Self::Failure> {
        self.state.view().poll.check()
    }

    fn quiesce<'a>(&'a mut self) -> Result<(), ready::Failures<Self::Failure>>
    where
        C::Context: 'a,
    {
        self.check()?;
        let parts = self.state.edit();
        if let ops::ControlFlow::Break(()) = parts.poll.execute(
            || {
                let reads = parts.reads.files().map(|id| (id, change::Filter::Read));
                let writes = parts.retries.files().map(|id| (id, change::Filter::Write));
                parts.files.try_delete(reads.chain(writes))?.commit();
                Ok::<(), change::Index<_>>(())
            },
            Self::Failure::backing,
        ) {
            return parts.poll.check().map_err(ready::Failures::from);
        }
        parts.poll.revoke(parts.files)?;
        parts.reads.clear();
        parts.retries.clear();
        Ok(())
    }

    fn close<'a>(&'a mut self, slot: Self::Slot) -> Self::Closed
    where
        C::Context: 'a,
    {
        if let Some(raw) = self.state.view().files.raw(slot) {
            let parts = self.state.edit();
            match parts.poll.execute(
                || parts.reads.remove_fd(raw, parts.files),
                Self::Failure::backing,
            ) {
                ops::ControlFlow::Continue(Some(target)) => self.pending.suppress(target),
                ops::ControlFlow::Continue(None) | ops::ControlFlow::Break(()) => {}
            }
            if !self.state.view().poll.is_failed() {
                match retry::raw::Write::take_fd(&mut self.state.write(), raw) {
                    ops::ControlFlow::Continue(Some(target)) => self.pending.suppress(target),
                    ops::ControlFlow::Continue(None) | ops::ControlFlow::Break(()) => {}
                }
            }
        }
        let file = self.state.edit().files.close(slot);
        closed::Completed {
            slot,
            file,
            reactor: self.check(),
        }
    }
}

impl<C: ready::Driver> Close for Engine<C> {
    type State = Infallible;

    /// Readiness syscalls finish before returning; kevent retains only integer identities.
    /// Closing the queue and descriptors releases their queued buffers and receive pool.
    fn try_close(self) -> Result<(), Infallible> {
        Ok(())
    }
}

impl<C: ready::Driver> shutdown::Shutdown for Engine<C> {
    type State = Infallible;

    /// The unallocated projection and engine's receive pool form one native owner.
    /// Synchronous syscalls retain no borrowed pointer after they return.
    fn try_shutdown(self, _: Self::Context) -> Result<(), Self::State> {
        self.try_close()
    }
}

impl<C: ready::Driver> complete::Observe for Engine<C> {
    type Observation = C::Observation;
}

mod raw;
