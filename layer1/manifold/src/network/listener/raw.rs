use std::pin;

use dope_core::driver::{route, route::Identity, schedule};
use dope_net::link::egress::{data, data::exec};
use o3::buffer::{hold, segments::scatter};

use crate::{
    app::{dispatch, env},
    network::{
        listener,
        listener::{
            conn, handler,
            runtime::control,
            writer::{memory, memory::release::Lease, phase},
        },
    },
};

pub struct Owned<'p, 's, 'd, ID: Identity> {
    slot: Lease<'p, 's, 'd, memory::Header, ID>,
    len: memory::NonZero,
}

impl<'p, 's, 'd, ID: Identity> Owned<'p, 's, 'd, ID> {
    pub(in crate::network::listener) fn new(
        slot: Lease<'p, 's, 'd, memory::Header, ID>,
        len: memory::NonZero,
    ) -> Self {
        Self { slot, len }
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: NonZero is constructible only within the fixed listener prefix
        // capacity owned by this committed header lease.
        unsafe {
            self.slot
                .get()
                .as_slice()
                .get_unchecked(..self.len.transfer().widen().get())
        }
    }
}

// SAFETY: The committed header lease and bounded prefix fix the source bytes until release.
unsafe impl<'d, ID: Identity> data::raw::Payload<'d> for Owned<'_, '_, 'd, ID> {
    type Mode = exec::Fallible;
    type Cursor<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn charge(&self) -> hold::Charge {
        hold::Charge::from_owner(self.as_slice().len(), memory::WRITE_BUF_CAP)
    }

    fn start(&self) -> Self::Cursor<'_> {
        self.as_slice()
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        let bytes = self.as_slice();
        offset.tail(bytes)
    }

    unsafe fn resume(
        &self,
        _: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: the caller retains this unchanged header and proves its nonempty suffix.
        unsafe { self.as_slice().get_unchecked(offset.get()..) }
    }
}

/// Restricted access view for an installed listener application.
/// # Safety
/// The view cannot move or release retained storage, nor extend `Loan` to `'d`.
pub unsafe trait Control<'d, ID: Identity>: handler::Application<'d, ID> {
    type View<'step>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(app: dispatch::raw::Loan<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step;
}

/// A listener borrow paired with one route- and generation-branded wake.
#[must_use]
pub struct Resume<'a, 'n, 'p, 's, 'w, 'd: 'a + 'w, ID: Identity, A, E>
where
    A: handler::Application<'d, ID> + handler::Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    owner: pin::Pin<&'a mut listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>>,
    id: conn::Id<'d, ID>,
}

impl<'a, 'n, 'p, 's, 'w, 'd: 'a + 'w, ID: Identity, A, E> Resume<'a, 'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID> + handler::Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    pub fn from_target(
        owner: pin::Pin<&'a mut listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>>,
        target: route::Target<'d, ID>,
    ) -> Option<Self> {
        let key = owner
            .as_ref()
            .project_ref()
            .owner
            .pool()
            .inspect()
            .by_target(target)?;
        Some(Self {
            owner,
            id: conn::Id::from_key(key),
        })
    }

    pub fn from_id(
        owner: pin::Pin<&'a mut listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>>,
        id: conn::Id<'d, ID>,
    ) -> Self {
        Self { owner, id }
    }

    pub fn id(&self) -> conn::Id<'d, ID> {
        self.id
    }

    pub fn listener(&self) -> &listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E> {
        self.owner.as_ref().get_ref()
    }

    /// # Safety
    /// `owner` must remain the listener installed for `driver` through this call.
    pub unsafe fn drive<'turn>(
        self,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, dispatch::raw::Retained>,
    ) {
        let Self { owner, id } = self;
        let target = id.key().target();
        // SAFETY: the caller retains this installed owner for the id's exact route.
        unsafe { <A as handler::raw::Activate<'w, 'd, ID>>::activate(owner, target, turn, driver) }
    }
}

use dope_core::driver::schedule::{Progress, Turn};
use dope_net::link::pool::pending;

use crate::{
    app::dispatch::event,
    network::listener::{Listener, handler::Runtime},
};
// SAFETY: Control cannot move or release the listener or retained buffers.
unsafe impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> dispatch::raw::Control<'d>
    for Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A: Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    type View<'step>
        = listener::Control<'step, 'n, 'p, 's, 'w, 'd, ID, A, E>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: dispatch::raw::Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        listener::Control { inner: owner }
    }
}
// SAFETY: Listener owns accept, ingress, send, and deadline phases through finish.
unsafe impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> dispatch::raw::Manifold<'d>
    for Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A: Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    type Route = ID;
    type Dispatch = event::Timed<dispatch::raw::Retained>;
    type Install = dispatch::raw::Omit;
    type Finish = Self;
    type Activate = dispatch::raw::Retained;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Retained;

    const COORDS: usize = 0;

    fn shutdown<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        turn: Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let mut this = self;
        {
            let fields = this.as_mut().project();
            fields.accept.stop_accept(driver);
            fields.schedule.shutdown();
        }
        control::shutdown(this.as_mut(), turn, driver);
        this
    }
    unsafe fn pre_park<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        turn: Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        use crate::network::listener::runtime::Phase;

        let mut this = self;
        control::admit(this.as_mut(), turn.reborrow(), driver);
        control::shutdown(this.as_mut(), turn.reborrow(), driver);
        let now = driver.driver_ref().clock().now();
        for _ in 0..Phase::COUNT {
            let phase = this.as_mut().project().schedule.next_phase();
            match phase {
                Phase::Accept => {
                    this.as_mut().arm_accept(turn.reborrow().maint(), driver);
                }
                Phase::Ingress => {
                    this.as_mut().flush_recv(turn.reborrow(), driver);
                }
                Phase::Credit => this
                    .as_ref()
                    .project_ref()
                    .owner
                    .pool()
                    .credit_step(driver.region_token(), turn.reborrow().maint()),
                Phase::Dirty => phase::flush_dirty(this.as_mut(), turn.reborrow(), driver),
                Phase::Deadline => {
                    control::expire(this.as_mut(), now, turn.reborrow(), driver);
                }
            }
            if turn.reborrow().maint().remaining() == 0 {
                return this;
            }
        }
        this
    }
}
// SAFETY: unit selection binds every query to this exact pinned listener.
unsafe impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> dispatch::raw::Probe<'d, Self>
    for Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A: Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> Progress<'d> {
        let this = call.owner().project_ref();
        if !pending::Pending::of(this.owner.pool()).is_empty()
            || this.owner.pool().inspect().has_rearm()
            || this.owner.pool().inspect().has_credit()
        {
            return Progress::Runnable;
        }
        if this.schedule.is_sealing() {
            return Progress::Runnable;
        }
        let until = [this.owner.earliest(), A::deadline(this.app, call.clock())]
            .into_iter()
            .flatten()
            .min();
        let pool = if !this.owner.pool().inspect().is_empty() {
            Progress::waiting(call.region())
        } else {
            Progress::Quiescent
        };
        let io = this.accept.progress(call.region()).reduce(pool);
        match until {
            Some(until) => io.reduce(Progress::until(until)),
            None => io,
        }
    }

    fn stop(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> Progress<'d> {
        let this = call.owner().project_ref();
        if !pending::Pending::of(this.owner.pool()).is_empty()
            || this.owner.pool().inspect().has_rearm()
            || this.owner.pool().inspect().has_credit()
        {
            return Progress::Runnable;
        }
        if this.schedule.is_closing() {
            return Progress::Runnable;
        }
        if this.schedule.is_done()
            && (this.accept.is_active() || !this.owner.pool().inspect().is_empty())
        {
            let waiting = this
                .accept
                .progress(call.region())
                .reduce(Progress::waiting(call.region()));
            match this.owner.earliest() {
                Some(until) => waiting.reduce(Progress::until(until)),
                None => waiting,
            }
        } else {
            Self::run(call)
        }
    }
}
// SAFETY: activation is bound to the listener's canonical installed route.
unsafe impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> dispatch::raw::Activate<'d, ID>
    for Listener<'n, 'p, 's, 'w, 'd, ID, A, E>
where
    A: handler::Application<'d, ID>,
    A: Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    type Access = dispatch::raw::Retained;

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        target: route::Target<'d, ID>,
        turn: Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        // SAFETY: dispatch admitted this pinned listener and its canonical target together.
        unsafe { <A as handler::raw::Activate<'w, 'd, ID>>::activate(self, target, turn, driver) }
    }
}
