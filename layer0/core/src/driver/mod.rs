use std::{marker, mem, num, ops::ControlFlow, ptr, task, time};

use dope_sys::{engine, io::reactor::ready::delivery::Quota as _, kernel};
use o3::{
    base::cell::region,
    buffer::{self, storage::provided},
};
use route::table;

use crate::{
    Backend, Engine, driver,
    driver::{
        life::{
            domain::runtime::Runtime,
            native,
            native::{create, host::Host},
        },
        receive::access,
        result::poll,
        retained::{flight, flight::cancel},
        route::{kind, queues},
        schedule::{
            clock,
            clock::{timer, timer::heap},
            ready,
            ready::{task::owner, wake::resident::slot},
            sequence,
            state::scope,
        },
    },
    io,
    io::{
        event::{
            batch,
            net::{accept, recv},
        },
        fd::{access::Access, outbound},
    },
    random,
};

pub mod life;
pub mod receive;
pub mod result;
pub mod retained;
pub mod route;
pub mod schedule;
pub mod settings;
pub mod storage;

use result::{IoOperation, Opcode, SubmitError};

type Buffer = <Engine as engine::Buffer>::Token;

/// Keeps every derived capability in the exact driver lifetime that issued it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct Invariant<'d>(marker::PhantomData<fn(&'d ()) -> &'d ()>);

#[must_use = "application binding must be consumed during startup"]
pub struct Binding<'d> {
    timer: timer::Timer<'d>,
    owners: owner::Capacity,
}

impl<'d> Binding<'d> {
    #[doc(hidden)]
    pub const fn new(timer: timer::Timer<'d>, owners: owner::Capacity) -> Self {
        Self { timer, owners }
    }

    pub const fn timer(&self) -> timer::Timer<'d> {
        self.timer
    }

    pub const fn owners(&self) -> owner::Capacity {
        self.owners
    }

    pub fn sequence(&self) -> sequence::Block<'d> {
        sequence::Block::new(self.timer.driver)
    }
}

#[repr(transparent)]
pub struct Driver {
    runtime: Runtime,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Reference<'d> {
    shared: provided::Root<'d, Buffer, Shared, ()>,
    pub(crate) brand: Invariant<'d>,
}

impl<'d> Reference<'d> {
    pub fn outbound_cap(self) -> table::Capacity {
        let arena = self.ready().arena();
        arena.descriptors().outbound_cap()
    }

    pub fn accept_cap(self) -> table::Capacity {
        let arena = self.ready().arena();
        arena.descriptors().accept_cap()
    }

    fn new(shared: provided::Root<'d, Buffer, Shared, ()>) -> Self {
        Self {
            shared,
            brand: Invariant(marker::PhantomData),
        }
    }

    pub const fn ready(self) -> ready::Context<'d> {
        ready::Context::new(self)
    }

    pub const fn targets<Tag: route::Tag>(self) -> route::Space<'d, Tag> {
        route::Space {
            driver: route::Brand::new(self.brand),
        }
    }

    pub const fn scheduler(self) -> schedule::Context<'d> {
        schedule::Context::new(self)
    }

    /// Issues one identity without reserving a scheduler's local block.
    pub fn try_next_id(self) -> Result<sequence::Id<'d>, sequence::Exhausted> {
        let raw = self
            .shared
            .context()
            .sequence
            .next()
            .ok_or(sequence::Exhausted)?;
        Ok(sequence::Id::new(self, raw))
    }

    pub fn clock(self) -> clock::Clock<'d, 'd> {
        self.scheduler().clock()
    }

    pub const fn timer(self) -> timer::Timer<'d> {
        timer::Timer::new(self)
    }

    pub fn maintenance(self) -> schedule::Progress<'d> {
        if self.has_deferred() {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }

    pub(crate) const fn receive(self) -> access::Access<'d> {
        access::Access::new(self)
    }

    pub(crate) fn has_deferred(self) -> bool {
        self.receive().owners().has_returned()
            || self.descriptors().retirement().has_pending()
            || self.shared.context().retire.has_pending()
    }

    pub(crate) const fn descriptors(self) -> Access<'d> {
        Access::new(self)
    }

    pub(crate) const fn outbound(self) -> outbound::Context<'d> {
        outbound::Context::new(self)
    }

    #[doc(hidden)]
    pub fn is_same(self, other: Reference<'_>) -> bool {
        ptr::from_ref(self.shared.context()).addr() == ptr::from_ref(other.shared.context()).addr()
    }

    #[doc(hidden)]
    pub fn random_seed(self) -> &'d random::Seed {
        self.shared.context().random_seed()
    }
}

const _: () = {
    assert!(mem::size_of::<IoOperation>() == 1);
    assert!(mem::align_of::<IoOperation>() == 1);
    assert!(mem::size_of::<Opcode>() == mem::size_of::<<crate::Backend as kernel::Source>::Code>());
    assert!(
        mem::align_of::<Opcode>() == mem::align_of::<<crate::Backend as kernel::Source>::Code>()
    );
    assert!(mem::size_of::<Driver>() == mem::size_of::<Runtime>());
    assert!(mem::align_of::<Driver>() == mem::align_of::<Runtime>());
    assert!(mem::size_of::<Reference<'_>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Reference<'_>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Invariant<'_>>() == 0);
    assert!(mem::align_of::<Invariant<'_>>() == 1);
    assert!(mem::size_of::<Context<'_, '_>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Context<'_, '_>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<SubmitError>() == 1 && mem::align_of::<SubmitError>() == 1);
    assert!(
        mem::size_of::<Result<flight::Flight<'static>, SubmitError>>()
            == mem::size_of::<(flight::Flight<'static>, SubmitError)>()
    );
    assert!(
        mem::align_of::<Result<flight::Flight<'static>, SubmitError>>()
            == mem::align_of::<flight::Flight<'static>>()
    );
};

#[repr(transparent)]
#[non_exhaustive]
pub struct Context<'a, 'd> {
    pub storage: storage::Context<'a, 'd>,
}

impl<'a, 'd> Context<'a, 'd> {
    fn new(local: &'a mut life::Local<'d>) -> Self {
        Self {
            storage: storage::Context::new(local),
        }
    }

    /// Reports one exact cancellation rejection to this scope's mandatory observer.
    pub fn rejected(&mut self, failure: cancel::Failure) {
        let report = io::Reclaimed::Cancellation(failure);
        (self.storage.0.lifecycle.report)(report);
    }

    /// Reports a consumed accept failure with its original driver-bound target.
    pub fn accept_error(
        &mut self,
        target: route::Erased<'d, kind::Accept>,
        cause: accept::Failure,
        flow: batch::Flow,
    ) {
        (self.storage.0.lifecycle.report)(io::Reclaimed::AcceptRoute {
            target: target.into_token(),
            cause,
            flow,
        });
    }

    /// Reclaims a terminal routed receive and reports its original target and outcome.
    pub fn retire(&mut self, event: io::Event<'d>) -> ControlFlow<io::Event<'d>> {
        use io::event::Kind;

        let done = match event.into_kind() {
            Kind::RetiredRecv(done) => done,
            kind => return ControlFlow::Break(kind.into()),
        };
        self.reclaim(done);
        ControlFlow::Continue(())
    }

    /// Consumes one routed receive, releases its owner, and reports the exact result.
    pub fn reclaim(&mut self, done: recv::Completion<'d>) {
        use io::transfer::Len;
        use recv::Outcome;

        let (target, flow, outcome) = done.into_parts();
        let result = match outcome {
            Outcome::Data(buffer) => {
                let length = buffer.len();
                Ok(length)
            }
            Outcome::Eof => Ok(Len::ZERO),
            Outcome::Cancelled => Err(crate::Errno::CANCELED),
            Outcome::Exhausted(proof) => Err(proof.retire()),
            Outcome::Starved(error) => Err(error.into()),
            Outcome::Failed(error) => Err(error),
        };
        (self.storage.0.lifecycle.report)(io::Reclaimed::RecvRoute {
            target: target.into_token(),
            result,
            flow,
        });
    }

    pub fn reborrow(&mut self) -> Context<'_, 'd> {
        Context::new(&mut *self.storage.0)
    }

    pub(crate) fn native(&mut self) -> native::Access<'_, 'd> {
        native::Access::new(&mut *self.storage.0)
    }

    pub fn region_token(&mut self) -> &mut region::Token<region::Identity<'d>> {
        &mut self.storage.0.region.0
    }

    pub fn state_token(&mut self) -> &mut region::Token<region::Identity<'d, 1>> {
        &mut self.storage.0.region.1
    }

    pub fn into_regions(
        self,
    ) -> &'a mut (
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ) {
        &mut self.storage.0.region
    }

    pub fn regions(
        &self,
    ) -> &(
        region::Token<region::Identity<'d>>,
        region::Token<region::Identity<'d, 1>>,
    ) {
        &self.storage.0.region
    }

    pub fn receive(&self) -> settings::Receive {
        self.storage.receive()
    }

    pub fn driver_ref(&self) -> Reference<'d> {
        self.storage.0.driver
    }
}

impl<'a, 'd> Context<'a, 'd> {
    pub fn dispatch(
        &mut self,
        work: scope::Reactor<'_, 'd>,
        emit: impl FnMut(io::Event<'d>, &mut Self) -> ControlFlow<io::Event<'d>>,
    ) -> poll::Dispatch<'d> {
        let work = native::Backend::new(work);
        // SAFETY: Context retains the same Local root, registered storage, and
        // branded flight owners throughout native resolution and host emission.
        let turn: <Backend as engine::Dispatch<'_, Host<Backend>, Self>>::Turn<_> =
            unsafe { engine::raw::Dispatch::<'_, Host<Backend>, Self, _>::new(self, work) };
        engine::raw::Dispatch::<'_, Host<Backend>, Self, _>::dispatch(turn, emit)
    }

    pub fn commit(&mut self, work: scope::Reactor<'_, 'd>) -> Result<poll::Commit, poll::Error> {
        let quota = native::Backend::new(work);
        quota.reclaim(self);
        let retry = self.storage.0.retry;
        let owner = self.driver_ref();
        let mut native = self.native();
        // SAFETY: Context retains this driver's registered storage and branded
        // flight owners throughout maintenance and native completion processing.
        let state = unsafe {
            engine::raw::Poll::commit(native.as_mut(), quota, || retry || owner.has_deferred())
        }
        .map_err(poll::Error::from_backend)?;
        self.storage.0.retry = false;
        Ok(match state {
            task::Poll::Ready(()) => poll::Commit::Drained,
            task::Poll::Pending => poll::Commit::Pending,
        })
    }

    pub fn wait(
        &mut self,
        work: scope::Reactor<'_, 'd>,
        timeout: Option<time::Duration>,
    ) -> Result<(), poll::Error> {
        let quota = native::Backend::new(work);
        quota.reclaim(self);
        let retry = self.storage.0.retry;
        let owner = self.driver_ref();
        let pending = self.storage.0.lifecycle.pending;
        let mut native = self.native();
        // SAFETY: Context retains this driver's registered storage and branded
        // flight owners throughout maintenance and native completion processing.
        unsafe {
            engine::raw::Poll::wait(native.as_mut(), quota, timeout, || {
                retry || pending || owner.has_deferred() || owner.ready().has_ready()
            })
        }
        .map_err(poll::Error::from_backend)?;
        self.storage.0.retry = false;
        Ok(())
    }
}

pub(crate) struct Shared {
    random_seed: random::Seed,
    pub(in crate::driver) schedule: schedule::State,
    pub(in crate::driver) receive: crate::Permits,
    buffer: <Engine as engine::Buffer>::Context,
    pub(in crate::driver) retire: queues::Pending,
    pub(in crate::driver) sequence: sequence::Owner,
    timer: heap::raw::Store<time::Instant, slot::Slot, timer::Tag>,
}

impl Shared {
    pub(in crate::driver) fn try_new(
        seed: random::Seed,
        plan: settings::Plan,
        failure: &mut Option<result::CreateError>,
    ) -> Result<(provided::Arena<driver::Buffer, Self, ()>, Engine), &mut result::CreateError> {
        let config = *plan.config();
        let initialize = match provided::Arena::<driver::Buffer, Self, ()>::try_allocate(
            config.receive().count().into(),
        ) {
            Ok(initialize) => initialize,
            Err(error) => return Err(failure.insert(error.into())),
        };
        let schedule = match schedule::State::try_new(plan) {
            Ok(schedule) => schedule,
            Err(error) => return Err(failure.insert(error.into())),
        };
        let timer = match heap::raw::Store::try_new(config.schedule().timers().count().get()) {
            Ok(timer) => timer,
            Err(error) => return Err(failure.insert(error.into())),
        };
        // SAFETY: all fallible host allocation precedes native creation.
        // This infallible assembly retains the returned context beside
        // its backend throughout the same driver generation and teardown.
        let (backend, buffer, receive) =
            match unsafe { <Backend as engine::raw::Create<Host<Backend>, _>>::try_new(&config) } {
                Ok(created) => created,
                Err(error) => return Err(failure.insert(create::Created::from(error).0)),
            };
        Ok((
            initialize(Self {
                random_seed: seed,
                schedule,
                receive,
                buffer,
                retire: queues::Pending::new(),
                sequence: sequence::Owner::new(),
                timer,
            }),
            backend,
        ))
    }

    pub(in crate::driver) const fn random_seed(&self) -> &random::Seed {
        &self.random_seed
    }

    pub(crate) fn buffer(&self) -> &<Engine as engine::Buffer>::Context {
        &self.buffer
    }

    pub(crate) fn into_buffer(self) -> <Engine as engine::Buffer>::Context {
        self.buffer
    }

    pub(in crate::driver) fn timer(
        &self,
    ) -> &heap::raw::Store<time::Instant, slot::Slot, timer::Tag> {
        &self.timer
    }
}

impl buffer::raw::Fixed<driver::Buffer> for Shared {
    fn resident(&self) -> num::NonZero<u32> {
        buffer::raw::Fixed::resident(&self.buffer)
    }

    unsafe fn project<'a, 'id>(
        &'a self,
        item: &'a mut driver::Buffer,
        limit: buffer::raw::Limit<'id>,
    ) -> buffer::raw::Projection<'a, 'id> {
        // SAFETY: Shared retains this exact native context beside its arena;
        // the caller supplies its uniquely owned completed item and resident limit.
        unsafe { buffer::raw::Fixed::project(&self.buffer, item, limit) }
    }
}
