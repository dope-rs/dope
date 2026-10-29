use std::{cell, cell::Cell, convert, pin, task, task::Poll};

use dope::{
    core::driver::{schedule, schedule::Progress, settings, settings::Routes},
    runtime::executor,
};
use dope_fiber::{
    abi, context, reply,
    reply::{
        Arena, Work, layout,
        layout::{Capacity, Data, Entries, Lanes, Layout, Limit},
        recv,
        recv::outcome::Terminal,
        retain, usage,
        usage::{Bytes, Usage, Values},
    },
};
use o3::{alloc, base::cell::region, mem::rc};

pub enum Fixture<'a> {
    Queue(&'a Drops),
    Slot(&'a Drops),
    Lanes(&'a Drops),
    Mixed(&'a Drops),
}

pub struct State<'a, 'scope, 'arena, 'credits, 'region> {
    pub(crate) arena: &'scope reply::Arena<'arena, 'credits, 'region, Counted<'a>>,
    pub(crate) drops: &'a Drops,
}

pub(crate) fn limit(data: usize, bytes: usize, values: usize) -> layout::Limit {
    Limit::new(Data::new(data), Bytes::new(bytes), Values::new(values))
}

pub(crate) fn capacity(
    lanes: usize,
    entries: usize,
    data: usize,
    bytes: usize,
    values: usize,
) -> layout::Capacity {
    Capacity::new(
        Lanes::new(lanes),
        Entries::new(entries),
        Data::new(data),
        Bytes::new(bytes),
        Values::new(values),
    )
}

pub fn layout(lanes: usize, entries: usize) -> Result<layout::Layout, layout::Error> {
    Layout::new(
        capacity(lanes, entries, entries, entries, entries),
        limit(1, 1, 1),
    )
}

pub struct Pending;

pub struct OwnerTag<'a, T>(pub &'a T);

pub struct Finish;

impl<'s, 'arena, 'credits, 'region, D, E> recv::Extract<'s, 'arena, 'credits, 'region, D, E>
    for Finish
{
    type Output = Result<(), E>;
    type Context = ();
    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::Collected;

    fn extract(
        _: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, D, E>,
    ) -> Option<Self::Output> {
        Some(match slot.terminal()? {
            Terminal::Complete(_) => Ok(()),
            Terminal::Failed(fault) => Err(fault.into_error()),
        })
    }
}

impl<'s, 'arena, 'credits, 'region> recv::Extract<'s, 'arena, 'credits, 'region, ()> for Pending {
    type Output = convert::Infallible;
    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::Unit;

    type Context = ();

    fn extract(
        _: &Self::Context,
        _: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, ()>,
    ) -> Option<Self::Output> {
        None
    }
}

pub struct Drain<'s, 'a, 'scope, 'arena, 'credits, 'region>(
    pub(crate) &'s State<'a, 'scope, 'arena, 'credits, 'region>,
);

#[derive(Default)]
pub struct Releases<'arena> {
    pub(crate) lanes: cell::Cell<Vec<reply::Released<'arena>>>,
    pub(crate) max_drops: cell::Cell<usize>,
}

impl<'s, 'a, 'scope, 'arena, 'credits, 'region> Drain<'s, 'a, 'scope, 'arena, 'credits, 'region> {
    pub const fn new(state: &'s State<'a, 'scope, 'arena, 'credits, 'region>) -> Self {
        Self(state)
    }
}

impl<'region> abi::Fiber<'region> for Drain<'_, '_, '_, '_, '_, 'region> {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'region, Self>) -> task::Poll<Self::Output> {
        let (this, context) = call.into_parts();
        if matches!(this.0.arena.work(), Work::Idle) {
            return Poll::Ready(());
        }
        context.as_ref().completion().wake();
        Poll::Pending
    }
}

impl<'arena> Releases<'arena> {
    pub const fn new() -> Self {
        Self {
            lanes: Cell::new(Vec::new()),
            max_drops: Cell::new(0),
        }
    }

    pub fn take(&self) -> Vec<reply::Released<'arena>> {
        self.lanes.take()
    }

    pub fn max_drops(&self) -> usize {
        self.max_drops.get()
    }

    pub fn record_drops(&self, drops: usize) {
        self.max_drops.set(self.max_drops.get().max(drops));
    }

    pub fn push(&self, released: reply::Released<'arena>) {
        let mut releases = self.lanes.take();
        releases.push(released);
        self.lanes.set(releases);
    }
}

impl<'a> Fixture<'a> {
    pub fn try_with<'region>(
        self,
        proof: region::Proof<region::Identity<'region>>,
        run: impl for<'scope, 'arena, 'credits> FnOnce(State<'a, 'scope, 'arena, 'credits, 'region>),
    ) -> Result<(), Build> {
        let (capacity, limit, drops) = match self {
            Self::Queue(drops) => (capacity(1, 4, 4, 1, 4), limit(4, 1, 4), drops),
            Self::Slot(drops) => (capacity(1, 1, 4, 1, 4), limit(4, 1, 4), drops),
            Self::Lanes(drops) => (capacity(2, 2, 2, 2, 2), limit(1, 1, 1), drops),
            Self::Mixed(drops) => (capacity(2, 2, 6, 6, 6), limit(3, 3, 3), drops),
        };
        let layout = Layout::new(capacity, limit).map_err(Build::Layout)?;
        Arena::try_with(layout, proof, |arena| run(State { arena, drops }))
            .map_err(Build::Allocation)
    }
}

#[derive(Default)]
pub struct Drops(pub(crate) cell::Cell<usize>);

pub struct Counted<'a> {
    pub(crate) failure: Option<u8>,
    pub(crate) bytes: usize,
    pub(crate) drops: &'a Drops,
}

pub struct Cause<'a> {
    pub(crate) value: u8,
    pub(crate) drops: &'a Drops,
}

impl Drops {
    pub const fn new() -> Self {
        Self(Cell::new(0))
    }

    pub fn count(&self) -> usize {
        self.0.get()
    }

    pub fn record(&self) {
        self.0.set(self.0.get() + 1);
    }
}

impl<'a> Counted<'a> {
    pub const fn data(drops: &'a Drops) -> Self {
        Self {
            failure: None,
            bytes: 0,
            drops,
        }
    }

    pub const fn mixed(drops: &'a Drops) -> Self {
        Self {
            failure: None,
            bytes: 1,
            drops,
        }
    }

    pub const fn is_data(&self) -> bool {
        self.failure.is_none()
    }
}

impl<'a> Cause<'a> {
    pub const fn new(value: u8, drops: &'a Drops) -> Self {
        Self { value, drops }
    }
}

impl usage::Footprint for Counted<'_> {
    fn footprint(&self) -> usage::Usage {
        Usage::new(
            Bytes::new(self.bytes),
            Values::new(usize::from(self.failure.is_none())),
        )
    }
}

impl<'a> From<&Cause<'a>> for Counted<'a> {
    fn from(cause: &Cause<'a>) -> Self {
        Self {
            failure: Some(cause.value),
            bytes: 0,
            drops: cause.drops,
        }
    }
}

impl Drop for Counted<'_> {
    fn drop(&mut self) {
        self.drops.record();
    }
}

pub struct Never;

pub struct Collected;

pub struct RetainOne;

pub struct DrainFailure;

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    Retain(rc::Error),
    Complete,
    Failed(Option<u8>),
    Data(u8),
    Terminal,
}

impl<'s, 'arena, 'credits, 'region, D> recv::Extract<'s, 'arena, 'credits, 'region, D> for Never {
    type Output = ();

    type Context = ();

    fn extract(
        _: &Self::Context,
        _: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, D>,
    ) -> Option<Self::Output> {
        None
    }
}

impl<'s, 'arena, 'credits, 'region, 'a> recv::Extract<'s, 'arena, 'credits, 'region, Counted<'a>>
    for Collected
{
    type Output = ();
    type Context = ();
    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::Collected;

    fn extract(
        _: &Self::Context,
        _: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, Counted<'a>>,
    ) -> Option<Self::Output> {
        None
    }
}

impl<'s, 'arena, 'credits: 'arena, 'region: 'credits, 'a>
    recv::Extract<'s, 'arena, 'credits, 'region, Counted<'a>> for RetainOne
{
    type Output = Result<retain::Retained<'arena, 'credits, 'region, Counted<'a>>, Error>;
    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, Counted<'a>>,
    ) -> Option<Self::Output> {
        match slot.terminal()? {
            Terminal::Complete(done) => done.try_retain().map_err(Error::Retain).transpose(),
            Terminal::Failed(fault) => Some(Err(Error::Failed(fault.into_error().failure))),
        }
    }
}

impl<'s, 'arena, 'credits, 'region, 'a>
    recv::Extract<'s, 'arena, 'credits, 'region, Counted<'a>, Counted<'a>> for DrainFailure
{
    type Output = Result<(usize, u8), Error>;

    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, Counted<'a>>,
    ) -> Option<Self::Output> {
        let mut fault = match slot.terminal()? {
            Terminal::Failed(fault) => fault,
            Terminal::Complete(_) => return Some(Err(Error::Complete)),
        };
        let mut values = 0;
        while let Some(item) = fault.pop() {
            if let Some(failure) = item.failure {
                return Some(Err(Error::Data(failure)));
            }
            values += 1;
        }
        let failure = fault.into_error().failure.ok_or(Error::Terminal);
        Some(failure.map(|failure| (values, failure)))
    }
}

pub struct Maintenance<'a, 'b, 'c, 'scope, 'arena, 'credits, 'region> {
    pub(crate) state: &'a State<'c, 'scope, 'arena, 'credits, 'region>,
    pub(crate) releases: &'b Releases<'arena>,
}

impl<'a, 'b, 'c, 'scope, 'arena, 'credits, 'region>
    Maintenance<'a, 'b, 'c, 'scope, 'arena, 'credits, 'region>
{
    pub const fn new(
        state: &'a State<'c, 'scope, 'arena, 'credits, 'region>,
        releases: &'b Releases<'arena>,
    ) -> Self {
        Self { state, releases }
    }
}

impl<'region> executor::raw::Layout<'region> for Maintenance<'_, '_, '_, '_, '_, '_, 'region> {
    type ParkPlan = ();
}

// SAFETY: the app has no pinned fields or routes; every callback consumes or
// confines its authority to the supplied driver turn.
unsafe impl<'region> executor::raw::App<'region> for Maintenance<'_, '_, '_, '_, '_, '_, 'region> {
    const ROUTES: settings::Routes = Routes::EMPTY;

    fn progress(
        app: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'region>,
    ) -> schedule::Progress<'region> {
        if matches!(app.state.arena.work(), Work::Pending) {
            Progress::Runnable
        } else {
            Progress::Quiescent
        }
    }

    fn stopping(
        app: pin::Pin<&Self>,
        _view: executor::raw::Progress<'_, '_, 'region>,
    ) -> schedule::Progress<'region> {
        match app.state.arena.work() {
            Work::Pending => Progress::Runnable,
            Work::Idle => Progress::Quiescent,
        }
    }
    const COORDS: usize = 0;

    fn select_park() -> Option<Self::ParkPlan> {
        Some(())
    }

    /// # Safety
    /// The root, prepared plan, and park authority belong to this installed owner.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: pin::Pin<&'app mut Self>,
        (): &mut Self::ParkPlan,
        park: executor::raw::Park<'driver, 'turn, 'root, 'region>,
    ) where
        'region: 'driver + 'turn + 'root,
    {
        let this = app.as_ref().get_ref();
        park.map(|turn, mut driver| {
            let before = this.state.drops.count();
            if let Some(value) = this.state.arena.maintain(turn.app(), driver.region_token()) {
                this.releases.push(value);
            }
            let after = this.state.drops.count();
            this.releases.record_drops(after.saturating_sub(before));
        });
    }
}

#[derive(Debug)]
pub enum Build {
    Layout(layout::Error),
    Allocation(alloc::Error),
}
