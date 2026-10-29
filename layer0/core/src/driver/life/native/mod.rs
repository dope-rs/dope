use std::mem;

use dope_sys::{
    engine,
    engine::admit,
    io::{
        reactor::{
            Full,
            ready::{delivery, store::Quota},
        },
        request::{Lower, Source},
        socket::option::{tuning, tuning::device},
    },
    kernel,
    memory::buffer,
    table::permit::Queue as _,
};
use o3::depot::arena::slab::Vacancy;
use schedule::state::{lanes, scope};

use crate::{
    Engine, driver,
    driver::{
        life,
        life::native::{request::permits, resolver::Resolver},
        receive, result,
        retained::{flight, flight::cancel},
        route,
        route::{indices, kind},
        schedule,
    },
    io::{
        fd::handles,
        socket,
        socket::{install::slot, option, pending},
    },
};

pub(crate) mod create;
pub(in crate::driver) mod drain;
pub(crate) mod host;
mod report;
pub(crate) mod request;
pub(crate) mod resolver;
mod retired;
pub(crate) use report::Report;

/// Exclusive native authority borrowed from one driver-local scope.
///
/// Its parts can only enter native code through the same `Local` root.
#[repr(transparent)]
pub(crate) struct Access<'a, 'd> {
    local: &'a mut life::Local<'d>,
}

impl<'a, 'd> Access<'a, 'd> {
    pub(in crate::driver) fn new(local: &'a mut life::Local<'d>) -> Self {
        Self { local }
    }

    pub(crate) fn try_admit(self) -> Result<permits::Admission<'a, 'd>, Full> {
        let permit = match admit::Admit::try_admit(&mut *self.local.backend) {
            Ok(permit) => permit,
            Err(full) => {
                self.local.retry = true;
                return Err(full);
            }
        };
        Ok(permits::Admission::new(
            permit,
            route::Brand::new(self.local.driver.brand),
        ))
    }

    pub(crate) fn try_cancel<C: kind::Class>(
        self,
        flight: &mut flight::Flight<'d, C>,
    ) -> Result<admit::Cancelled, cancel::Error> {
        let echo = flight.key();
        let pending = flight.is_pending();
        match C::KIND {
            kind::Kind::Accept => self.cancel::<admit::Pending<0>>(echo, pending, None),
            kind::Kind::Recv => self.cancel::<admit::Pending<1>>(echo, pending, None),
            kind::Kind::Send | kind::Kind::Connect => {
                self.cancel::<admit::Pending<2>>(echo, pending, None)
            }
            kind::Kind::Unspecified
            | kind::Kind::Socket
            | kind::Kind::Shutdown
            | kind::Kind::Open
            | kind::Kind::OneShot
            | kind::Kind::TaskQueue
            | kind::Kind::Tuning
            | kind::Kind::Write
            | kind::Kind::Sync => self.cancel::<admit::Immediate>(echo, pending, None),
        }
    }

    pub(crate) fn try_stop_at(
        self,
        flight: &mut flight::Flight<'d, kind::Connect>,
        fd: &handles::Descriptor<'d>,
    ) -> Result<admit::Cancelled, cancel::Error> {
        self.cancel::<admit::Pending<2>>(flight.key(), flight.is_pending(), Some(fd.slot()))
    }

    fn cancel<P: admit::Plan>(
        self,
        echo: flight::raw::Echo,
        pending: bool,
        slot: Option<handles::PhysicalSlot>,
    ) -> Result<admit::Cancelled, cancel::Error> {
        let native: Result<P::Guard<'_, host::Host<crate::Backend>, Engine>, _> = match slot {
            Some(slot) => admit::Reserve::try_at(&mut *self.local.backend, echo, pending, &slot),
            None => admit::Reserve::try_new(&mut *self.local.backend, echo, pending),
        };
        let native = native.map_err(|error| {
            if matches!(error, admit::Error::Full(_)) {
                self.local.retry = true;
            }
            cancel::Error::new(echo, error)
        })?;
        Ok(admit::Submission::submit(native))
    }

    fn submitted<T>(
        &mut self,
        result: Result<T, device::Failure<handles::Descriptor<'d>, Vacancy>>,
    ) -> Result<T, pending::Rejection<'d>> {
        result.map_err(|error| {
            if matches!(&error, device::Failure::Reserve(tuning::Error::Full(..))) {
                self.local.retry = true;
            }
            error.into()
        })
    }

    pub(crate) fn resolve(self) -> Resolver<'a, 'd> {
        Resolver::new(self.local)
    }

    pub(crate) fn reclaim(&mut self, limit: usize) {
        let driver = self.local.driver;
        let owners = driver.receive().owners();
        let mut count = 0;
        while count < limit {
            let Some(token) = owners.pop_returned() else {
                break;
            };
            // SAFETY: Local pairs this backend with its branded receive owners.
            // pop_returned transfers the unique slot after its last projection.
            unsafe { buffer::raw::Release::release(&mut *self.local.backend, token) };
            count += 1;
        }
        if count != 0
            && let Ok(permit) = crate::Permits::try_acquire()
        {
            receive::Proof::release(&permit, driver.receive().exhausted(), driver);
        }
    }

    pub(crate) fn install(
        self,
        hold: slot::Reserved<'d>,
        handle: socket::Handle,
    ) -> Result<handles::Descriptor<'d>, result::Kernel> {
        let state = &mut *self.local.backend;
        let slot = handles::OwnedSlot::from(hold.slot());
        if let Err(error) = engine::Install::install(state, slot, handle.into_inner()) {
            drop(hold);
            let cause: <crate::Backend as kernel::Source>::Failure<indices::SlotIndex> =
                error.into();
            return Err(result::Kernel::native(cause));
        }
        let (driver, slot) = hold.commit();
        Ok(handles::Descriptor::from_slot(slot, driver))
    }
}

const _: () = {
    assert!(mem::size_of::<Access<'static, 'static>>() == mem::size_of::<usize>());
};

impl AsMut<Engine> for Access<'_, '_> {
    fn as_mut(&mut self) -> &mut Engine {
        &mut self.local.backend
    }
}

/// Owns the routed option request throughout native application and settlement.
pub(crate) struct Apply<'a, 'd, Tag>
where
    Tag: route::Tag + route::Select<kind::Tuning>,
    'd: 'a,
{
    native: Access<'a, 'd>,
    slots: &'a flight::Slots<'d, Tag>,
    target: route::Target<'d, Tag>,
    fd: handles::Descriptor<'d>,
    opts: option::Set,
}

impl<'a, 'd, Tag> Apply<'a, 'd, Tag>
where
    Tag: route::Tag + route::Select<kind::Tuning>,
    'd: 'a,
{
    pub(crate) fn new(
        native: Access<'a, 'd>,
        slots: &'a flight::Slots<'d, Tag>,
        target: route::Target<'d, Tag>,
        fd: handles::Descriptor<'d>,
        opts: option::Set,
    ) -> Self {
        Self {
            native,
            slots,
            target,
            fd,
            opts,
        }
    }
}

// SAFETY: Local and the invariant descriptor lifetime bind one driver. Domain
// checked this descriptor's index inside the immutable FileSlots extent; both
// native targets below come from it. A ring backend creates its pinned tuning
// table with that same extent. There pending::Tuning owns the fd until
// cqe::Tuned::complete clears its state before completion. Drop leaves the
// Domain retirement queued while Engine::retire waits for that state's terminal;
// only then may close completion recycle the slot. Readiness options are applied
// synchronously. Local retains the backend throughout both native paths.
unsafe impl<'a, 'd, Tag, B> device::raw::Host<B, { option::MAX_STREAM_OPTIONS }>
    for Apply<'a, 'd, Tag>
where
    Tag: route::Tag + route::Select<kind::Tuning>,
    'd: 'a,
    Access<'a, 'd>: AsMut<B>,
{
    type File = handles::Descriptor<'d>;
    type Slot = <<crate::Backend as Source<request::Context>>::Raw as Lower>::Target;
    type Index = indices::SlotIndex;
    type Reserve = flight::Request<'a, 'd, Tag, kind::Tuning>;
    type Options<'o> = option::NonEmpty<'o>;
    type Permit = <Engine as engine::Cancel<indices::SlotIndex>>::Permit;
    type Output = option::Tuning<'d>;
    type Error = pending::Rejection<'d>;

    fn direct(
        mut self,
        run: impl for<'o> FnOnce(
            &mut B,
            Self::File,
            Self::Slot,
            Option<Self::Options<'o>>,
        ) -> Result<Self::File, (Self::File, dope_sys::Errno)>,
    ) -> Result<Self::Output, Self::Error> {
        let slot = <crate::Backend as Source<request::Context>>::target(self.fd.slot());
        let options = self.opts.non_empty();
        let fd = run(self.native.as_mut(), self.fd, slot, options)
            .map_err(|(fd, errno)| device::Failure::<_, Vacancy>::Option(fd, errno))?;
        Ok(option::Tuning::Applied(fd))
    }

    fn with(
        self,
        run: impl for<'o> FnOnce(
            &mut B,
            Self::File,
            Self::Slot,
            Self::Index,
            Option<Self::Options<'o>>,
            Self::Reserve,
        ) -> Result<
            device::Finish<Self::File, flight::Reserved<'d, kind::Tuning>, Self::Permit>,
            device::Failure<Self::File, Vacancy>,
        >,
    ) -> Result<Self::Output, Self::Error> {
        let Self {
            mut native,
            slots,
            target,
            fd,
            opts,
        } = self;
        let slot = <crate::Backend as Source<request::Context>>::target(fd.slot());
        let index = fd.index();
        let reserve = flight::Request::new(slots, target.operation::<kind::Tuning>());
        let opts = opts.non_empty();
        let result = run(native.as_mut(), fd, slot, index, opts, reserve);
        Ok(pending::Tuning::from_native(native.submitted(result)?))
    }
}

/// The only authority handed from common reactor maintenance to a backend.
pub(crate) struct Backend<'turn, 'd> {
    work: scope::Reactor<'turn, 'd>,
}

impl<'turn, 'd> Backend<'turn, 'd> {
    pub(crate) fn new(work: scope::Reactor<'turn, 'd>) -> Self {
        Self { work }
    }
}

impl<'d> delivery::Quota<driver::Context<'_, 'd>> for Backend<'_, 'd> {
    /// Returns native buffers and notifies current receive waiters.
    fn reclaim(&self, ctx: &mut driver::Context<'_, 'd>) {
        ctx.native().reclaim(schedule::LIMIT);

        let keep = self.share::<{ scope::REACTORS }, 1>(self.work.cursor());

        let limit = self.remaining().saturating_sub(keep);
        {
            let mut budget = self.work.budget::<drain::Lane>(limit);
            drain::Drain::reclaim(ctx, &mut budget);
        }
    }
}

impl<'turn, 'd> Quota for Backend<'turn, 'd> {
    type Budget<'a, Lane>
        = lanes::Budget<'a, 'd, Lane>
    where
        Self: 'a;

    fn remaining(&self) -> usize {
        self.work.remaining()
    }

    fn refund(self) {
        self.work.refund();
    }

    fn share<const N: usize, const I: usize>(&self, cursor: usize) -> usize {
        const {
            assert!(N != 0);
            assert!(I < N);
            assert!(N <= usize::MAX - I);
        }
        let total = self.remaining();
        let base = total / N;
        let extra = total % N;
        let span = (I + N - (cursor % N)) % N;
        base + if span < extra { 1 } else { 0 }
    }

    fn budget<Lane>(&self, limit: usize) -> Self::Budget<'_, Lane> {
        self.work.budget(limit)
    }
}
