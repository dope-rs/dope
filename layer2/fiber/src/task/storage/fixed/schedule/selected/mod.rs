use std::pin;

use o3::mem::{Lease, Return, lease};

use crate::{
    abi,
    core::driver::{
        retained,
        schedule::{app, app::step},
    },
    task::storage::fixed::{
        quota, schedule,
        schedule::{
            plane,
            plane::Active,
            queue, ready,
            ready::{Resident, TreeIndex},
            result::Outcome,
        },
        tree,
    },
};

pub mod route;

type Completed<'d, M> = Outcome<
    <M as tree::Member<'d>>::Target,
    <<M as tree::Member<'d>>::Fiber as abi::Fiber<'d>>::Output,
>;

type Borrowed<'a, R, Q> = pin::Pin<&'a mut Active<R, Q>>;

pub(super) trait Index<'d, R: schedule::PlaneTree<'d>> {
    fn into_root(self) -> ready::TreeIndex<R>;
}

pub(super) trait Split<'d, R: schedule::PlaneTree<'d>>: Index<'d, R> {
    type Left: Index<'d, R>;
    type Right: Index<'d, R>;

    fn split(self) -> Result<Self::Left, Self::Right>;
}

#[must_use = "dropping a selected task returns it to the ready set"]
pub struct Item<'plane, 'turn, 'd, R, Q = quota::Physical, S = queue::Flat<'d>>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
{
    claim: Claim<'plane, 'turn, 'd, R, Q, S, ready::TreeIndex<R>>,
}

impl<'plane, 'turn, 'd, R, Q, S> Item<'plane, 'turn, 'd, R, Q, S>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
{
    pub(super) fn new(
        dequeued: plane::Dequeued<'plane, 'd, R, Q, S>,
        work: app::Work<'turn, 'd>,
        permit: app::Permit<'turn, 'd>,
    ) -> Self {
        Self {
            claim: Claim::select(dequeued, work, permit),
        }
    }
}

impl<'plane, 'turn, 'd, M, Q, S> Item<'plane, 'turn, 'd, tree::Leaf<M>, Q, S>
where
    M: schedule::Ops<'d>,
    tree::Leaf<M>: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
{
    pub fn only(self) -> route::Entry<'plane, 'turn, 'd, tree::Leaf<M>, tree::Here, Q, S> {
        route::Entry {
            claim: self.claim.map(),
        }
    }
}

/// An occupied member borrowed from its selected plane.
///
/// The member and its index cannot be supplied independently or rebound to
/// another pool. The exclusive plane claim keeps this admission alive.
///
pub struct Entry<'a, M> {
    member: pin::Pin<&'a mut M>,
    index: Resident<M>,
}

/// A shared reborrow of an occupied member in a selected plane.
///
pub struct EntryRef<'a, M> {
    member: &'a M,
    index: Resident<M>,
}

/// Exclusive access to a live queue member in its owning route tree.
///
/// A numerical index does not grant live access to any tree of the same type.
///
pub struct Access<'a, R> {
    routes: pin::Pin<&'a mut R>,
    index: TreeIndex<R>,
}

impl<'a, M> Entry<'a, M> {
    pub(in crate::task::storage::fixed::schedule) fn into_parts(
        self,
    ) -> (pin::Pin<&'a mut M>, Resident<M>) {
        (self.member, self.index)
    }
}

impl<'a, M> EntryRef<'a, M> {
    pub(in crate::task::storage::fixed::schedule) fn into_parts(self) -> (&'a M, Resident<M>) {
        (self.member, self.index)
    }
}

impl<'a, R> Access<'a, R> {
    const fn new(routes: pin::Pin<&'a mut R>, index: TreeIndex<R>) -> Self {
        Self { routes, index }
    }

    pub(in crate::task::storage::fixed::schedule) fn from_ready<'d>(
        ready: plane::Ready<'a, R>,
    ) -> Self
    where
        R: schedule::PlaneTree<'d>,
    {
        let (routes, global) = ready.into_parts();
        // SAFETY: Ready can only be issued by Plane from its own live queue entry.
        let index = unsafe { ready::raw::admitted(global) };
        Self::new(routes, index)
    }
}

impl<'a, L, R> Access<'a, tree::Branch<L, R>> {
    pub(in crate::task::storage::fixed::schedule) fn split<'d>(
        self,
    ) -> Result<Access<'a, L>, Access<'a, R>>
    where
        L: schedule::PlaneTree<'d>,
        R: schedule::PlaneTree<'d>,
    {
        let (left, right) = self.routes.split();
        match self.index.split() {
            Ok(index) => Ok(Access::new(left, index)),
            Err(index) => Err(Access::new(right, index)),
        }
    }
}

impl<'a, M> Access<'a, tree::Leaf<M>> {
    pub(in crate::task::storage::fixed::schedule) fn entry(self) -> Entry<'a, M> {
        Entry {
            member: self.routes.storage(),
            index: self.index.resident(),
        }
    }
}

/// Owns the exact Active/dequeue pair published only after checked admission.
/// Exclusive ownership prevents removal/reuse; map/branch move both together.
/// drive_once lends a member without overlapping the enclosing Active loan.
pub(super) struct Claim<'plane, 'turn, 'd, R, Q, S, I>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
    I: Index<'d, R>,
{
    state: Lease<State<'plane, 'turn, 'd, R, Q, S, I>, lease::Open>,
}

struct State<'plane, 'turn, 'd, R, Q, S, I>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
    I: Index<'d, R>,
{
    active: Borrowed<'plane, R, Q>,
    dequeued: S::Dequeued<'plane, I>,
    work: app::Work<'turn, 'd>,
    permit: app::Permit<'turn, 'd>,
}

impl<'plane, 'turn, 'd, R, Q, S, I> Claim<'plane, 'turn, 'd, R, Q, S, I>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
    I: Index<'d, R>,
{
    const fn new(
        active: Borrowed<'plane, R, Q>,
        dequeued: S::Dequeued<'plane, I>,
        work: app::Work<'turn, 'd>,
        permit: app::Permit<'turn, 'd>,
    ) -> Self {
        Self {
            state: Lease::open(
                State {
                    active,
                    dequeued,
                    work,
                    permit,
                },
                (),
            ),
        }
    }

    fn map<J: Index<'d, R> + From<I>>(self) -> Claim<'plane, 'turn, 'd, R, Q, S, J> {
        let State {
            active,
            dequeued,
            work,
            permit,
        } = self.take();
        Claim::new(active, S::map(dequeued, J::from), work, permit)
    }

    fn take(self) -> State<'plane, 'turn, 'd, R, Q, S, I> {
        self.state.into_parts().0
    }
}

impl<'plane, 'turn, 'd, R, Q, S> Claim<'plane, 'turn, 'd, R, Q, S, TreeIndex<R>>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
{
    fn select(
        selected: plane::Dequeued<'plane, 'd, R, Q, S>,
        work: app::Work<'turn, 'd>,
        permit: app::Permit<'turn, 'd>,
    ) -> Self {
        let (active, dequeued) = selected.into_parts();
        // SAFETY: plane::Dequeued owns the exact Active/queue pairing. Its
        // queued admission was formed from a checked Path in these routes.
        let dequeued = S::map(dequeued, |global| unsafe { ready::raw::admitted(global) });
        Self::new(active, dequeued, work, permit)
    }
}

impl<'plane, 'turn, 'd, R, P, Q, S> Claim<'plane, 'turn, 'd, R, Q, S, route::Index<'d, R, P>>
where
    R: schedule::PlaneTree<'d> + tree::At<'d, P>,
    R::Member: schedule::Ops<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
{
    fn entry_ref(&self) -> EntryRef<'_, R::Member> {
        EntryRef {
            member: R::project_ref(&self.state.sink().active.as_ref().get_ref().routes),
            index: S::index(&self.state.sink().dequeued).resident,
        }
    }

    fn drive_once(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> (
        Borrowed<'plane, R, Q>,
        Completed<'d, R::Member>,
        step::Receipt<'turn, 'd>,
    ) {
        let State {
            mut active,
            dequeued,
            work,
            permit,
        } = self.take();
        let index = S::index(&dequeued).resident;
        let dequeued = S::map(dequeued, |index| index.into_root().global());
        let (routes, quota) = active.as_mut().split();
        let entry = Entry {
            member: R::project(routes),
            index,
        };
        let (outcome, receipt) = step::Receipt::delegate(permit, |permit| {
            schedule::Ops::drive_index::<S>(entry, dequeued, work, permit, driver)
        });
        if matches!(&outcome, Outcome::Complete { .. }) {
            *quota = quota::State::retire(*quota, quota::Retired::new());
        }
        (active, outcome, receipt)
    }

    fn step(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> (Completed<'d, R::Member>, step::Receipt<'turn, 'd>) {
        let (_, outcome, receipt) = self.drive_once(driver);
        (outcome, receipt)
    }
}

impl<'plane, 'turn, 'd, R, P, Q>
    Claim<'plane, 'turn, 'd, R, Q, queue::Flat<'d>, route::Index<'d, R, P>>
where
    R: schedule::PlaneTree<'d> + tree::At<'d, P>,
    R::Member: schedule::Ops<'d>,
    Q: quota::State,
{
    fn drive(self, driver: &mut retained::Context<'_, '_, 'd>) -> Completed<'d, R::Member> {
        let queue = self.state.sink().dequeued.owner();
        let (mut active, outcome, receipt) = self.drive_once(driver);
        if let Some(ready) = queue.snapshot()
            && let Some(global) = ready.peek()
        {
            ready.pause();
            let (routes, _) = active.as_mut().split();
            // SAFETY: queue was retained from this exact claim before driving;
            // the returned exclusive Active still owns every queued admission.
            let index = unsafe { ready::raw::admitted(global) };
            R::wake_global(Access::new(routes, index));
        }
        receipt.settle(step::Flow::Yield);
        outcome
    }
}

impl<'plane, 'turn, 'd, R, Q, S, I> Return for State<'plane, 'turn, 'd, R, Q, S, I>
where
    R: schedule::PlaneTree<'d>,
    Q: quota::State,
    S: queue::Backend<'d> + 'plane,
    I: Index<'d, R>,
{
    type Item = ();

    fn return_item(self, (): ()) {
        let State {
            mut active,
            dequeued,
            permit,
            ..
        } = self;
        let (routes, _) = active.as_mut().split();
        permit.run(|| {
            let dequeued = S::map(dequeued, Index::into_root);
            let root = *S::index(&dequeued);
            let dequeued = S::map(dequeued, TreeIndex::global);
            R::restore::<S>(Access::new(routes, root), dequeued);
        });
    }
}
