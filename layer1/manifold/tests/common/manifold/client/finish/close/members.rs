use std::{cell::Cell, convert::Infallible, net::SocketAddr, num::NonZero, rc::Rc, task::Poll};

use dope_core::{
    driver::{route::KeyTag, schedule::clock},
    io::socket::{self, option, option::Set},
};
use dope_manifold::{
    app::{dispatch::raw, env::Bundle},
    network::client,
    service::{
        self, Endpoint, Revision, Snapshot, discover,
        discover::{Action, Reason},
        observe, sync,
    },
    timing::Balanced,
};
use dope_net::{Transport, tcp::Tcp, wire::Identity};
use dope_runtime::{control, executor::root};
use dope_test::harness::{
    observe::Trace,
    signals::{Gate, Progress as _},
};
use o3::base::cell::region;

use crate::common::{
    contract::codec,
    manifold::client::{
        finish::close::observe::{Events, Session},
        side::limited::Limited,
    },
};

type Peers<const N: usize = 1> = Snapshot<Endpoint<u32, SocketAddr>, N>;

pub struct Source<const N: usize = 1> {
    pub(crate) pending: Rc<Cell<Option<Peers<N>>>>,
    pub(crate) failures: Gate,
}

impl<const N: usize> discover::Output for Source<N> {
    type Metadata<'d> = ();
    type Error = Infallible;
}

impl<'d, const N: usize> discover::RefreshAt<'d> for Source<N> {
    fn refresh_at(&mut self, _: clock::Moment<'d>, reason: discover::Reason) {
        if let Reason::Failure(_) = reason {
            self.failures.hit();
        }
    }
}

impl<'d, const N: usize> discover::PollAt<'d, u32, SocketAddr, N> for Source<N> {
    fn is_ready(&self, _: &region::Token<region::Identity<'d, 1>>) -> bool {
        let pending = self.pending.take();
        let ready = pending.is_some();
        self.pending.set(pending);
        ready
    }

    fn poll_at(
        &mut self,
        _: &mut region::Token<region::Identity<'d, 1>>,
        _: clock::Moment<'d>,
    ) -> discover::Action<'d, u32, SocketAddr, (), Infallible, N> {
        match self.pending.take() {
            Some(snapshot) => Action::Published {
                snapshot,
                metadata: (),
                poll_at: None,
            },
            None => Action::Pending { poll_at: None },
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum Notice {
    Resolved(Revision, Option<u32>),
    Stale(Revision),
    Expired(Revision),
    Reconciled(Revision, service::Change),
    Rejected(service::Reconcile),
}

pub struct Watch(pub(crate) Rc<Trace<Notice, 16>>);

impl<'d, const N: usize> observe::Audit<'d, u32, SocketAddr, Source<N>, N> for Watch {
    fn resolved(&mut self, _: &Source<N>, set: &Peers<N>, _: &()) {
        let id = set.endpoints().first().map(|entry| *entry.id());
        self.0.record(Notice::Resolved(set.revision(), id));
    }

    fn stale(&mut self, _: &Source<N>, rev: Revision) {
        self.0.record(Notice::Stale(rev));
    }

    fn expired(&mut self, _: &Source<N>, rev: Revision) {
        self.0.record(Notice::Expired(rev));
    }

    fn reconciled(&mut self, _: &Source<N>, rev: Revision, change: service::Change) {
        self.0.record(Notice::Reconciled(rev, change));
    }
}

impl<'d, const N: usize> observe::Failure<'d, u32, SocketAddr, Source<N>, N> for Watch {
    fn failed(&mut self, _: &Source<N>, error: Infallible) {
        match error {}
    }

    fn rejected(&mut self, _: &Source<N>, error: service::Reconcile) {
        self.0.record(Notice::Rejected(error));
    }
}

type Connection<'q, 'h, 'p, 'd, R> = client::Connector<
    'q,
    'h,
    'p,
    'd,
    'd,
    KeyTag<0>,
    2,
    Session<'d, codec::Outbound>,
    discover::Refreshing<'d, Source<2>>,
    R,
    u32,
    Watch,
    Bundle<Tcp, Identity, Balanced>,
    2,
>;

pub struct Check<'step, 'q, 'h, 'p, 'd: 'step, R: sync::Policy>(
    raw::Installed<'step, 'd, Limited<Connection<'q, 'h, 'p, 'd, R>>>,
);

// SAFETY: the view keeps installed storage private and only checks admission.
unsafe impl<'q, 'h, 'p, 'd, R: sync::Policy> raw::Control<'d>
    for Limited<Connection<'q, 'h, 'p, 'd, R>>
{
    type View<'step>
        = Check<'step, 'q, 'h, 'p, 'd, R>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: raw::Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        Check(owner)
    }
}

impl<R: sync::Policy> Check<'_, '_, '_, '_, '_, R> {
    fn check(&mut self, changes: &Trace<Notice, 16>, checked: &Gate, removed: SocketAddr) {
        let pass = checked.hits();
        match pass {
            0 => {
                if !changes
                    .values()
                    .expect("discovery observations")
                    .iter()
                    .any(|notice| {
                        matches!(notice, Notice::Reconciled(rev, _) if *rev == Revision::new(2))
                    })
                {
                    return;
                }
            }
            1 if R::AUTHORITATIVE => {}
            2 if !R::AUTHORITATIVE => {
                let unvisited = {
                    let (session, current) = self.0.as_mut().project().inner.session_admission();
                    let observed = Events::snapshot(&session.events);
                    let unvisited = *observed
                        .conn
                        .iter()
                        .find(|id| id.index().get() == 1)
                        .expect("second live lane");
                    assert!(observed.quarantined.iter().all(|(id, _, _)| *id != unvisited));
                    assert!(observed.closed.iter().all(|(id, _)| *id != unvisited));
                    assert!(observed.conn.iter().all(|id| !current(*id)));
                    unvisited
                };
                assert!(self.0.as_mut().project().inner.conn_mut(unvisited).is_some());
                checked.hit();
                return;
            }
            _ => return,
        }
        let (session, current) = self.0.as_mut().project().inner.session_admission();
        let observed = Events::snapshot(&session.events);
        let index = observed.peers[..2]
            .iter()
            .position(|peer| peer.into_std().expect("TCP peer") == removed)
            .expect("removed endpoint connected");
        let old = observed.conn[index];
        if pass == 0 {
            assert_eq!(
                session
                    .wait_reply
                    .as_ref()
                    .expect("withdrawal marker")
                    .hits(),
                0
            );
            assert_eq!(current(old), !R::AUTHORITATIVE);
            assert!(current(observed.conn[1 - index]));
            let mut observed = session.events.take();
            observed.replies = Some(1);
            session.events.set(observed);
        } else {
            let Some(&new) = observed.conn[2..]
                .iter()
                .find(|id| id.index() == old.index())
            else {
                return;
            };
            assert_ne!(new, old);
            assert!(!current(old));
            assert!(current(new));
        }
        checked.hit();
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime, driver = 'd)]
#[coordinate]
pub struct Admission<'q, 'h, 'p, 'd, R: sync::Policy> {
    #[pin]
    #[manifold(control)]
    pub(crate) pool: Limited<Connection<'q, 'h, 'p, 'd, R>>,
    #[dispatcher(state)]
    pub(crate) changes: Rc<Trace<Notice, 16>>,
    #[dispatcher(state)]
    pub(crate) checked: Gate,
    #[dispatcher(state)]
    pub(crate) removed: SocketAddr,
}

impl<'q, 'h, 'p, 'd, R: sync::Policy> Admission<'q, 'h, 'p, 'd, R> {
    fn coordinate(mut this: admission_coord::Fields<'_, 'q, 'h, 'p, 'd, R>) -> control::Flow {
        this.pool.check(this.changes, this.checked, *this.removed);
        control::Flow::Idle
    }
}

pub struct Until<F>(pub(crate) F);

impl<'d, F: Fn() -> bool> root::Root<'d> for Until<F> {
    type Output = ();

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<()> {
        let (this, wake, _, _) = context.into_parts();
        if this.0() {
            Poll::Ready(())
        } else {
            wake.wake();
            Poll::Pending
        }
    }
}

pub struct Address {
    pub(crate) addr: SocketAddr,
    pub(crate) chosen: Rc<Trace<SocketAddr, 4>>,
}

pub struct Net;

impl From<&Address> for socket::Inet {
    fn from(addr: &Address) -> Self {
        addr.chosen.record(addr.addr);
        Self::from(&addr.addr)
    }
}

impl Transport for Net {
    type Addr = Address;
    type Prepared = socket::Inet;
    type StreamConfig = ();

    fn options(_: ()) -> option::Set {
        Set::EMPTY
    }
}

pub struct DemandState {
    pub(crate) needed: Cell<bool>,
    pub(crate) pending: Cell<Option<u32>>,
    pub(crate) live: Cell<Option<u32>>,
    pub(crate) withdrawn: Cell<bool>,
}

impl Default for DemandState {
    fn default() -> Self {
        Self {
            needed: Cell::new(true),
            pending: Cell::new(Some(0)),
            live: Cell::new(Some(0)),
            withdrawn: Cell::new(false),
        }
    }
}

pub struct Demand(pub(crate) Rc<DemandState>);

impl<'d> discover::Demand<'d> for Demand {
    type Endpoint = u32;

    fn is_needed(&self, _: &region::Token<region::Identity<'d, 1>>) -> bool {
        let needed = self.0.needed.get();
        if !needed {
            self.0.withdrawn.set(true);
        }
        needed
    }

    fn is_eligible(
        &self,
        endpoint: &Self::Endpoint,
        _: &region::Token<region::Identity<'d, 1>>,
    ) -> bool {
        self.0.pending.get() == Some(*endpoint)
    }

    fn is_retained(
        &self,
        endpoint: &Self::Endpoint,
        _: &region::Token<region::Identity<'d, 1>>,
    ) -> bool {
        let retained = self.0.live.get() == Some(*endpoint);
        if !retained {
            self.0.withdrawn.set(true);
        }
        retained
    }
}

pub struct LimitedDemand {
    pub(crate) inner: Demand,
    pub(crate) chosen: Rc<Trace<SocketAddr, 4>>,
    pub(crate) limit: Rc<Cell<NonZero<usize>>>,
}

impl<'d> discover::Demand<'d> for LimitedDemand {
    type Endpoint = u32;

    fn is_needed(&self, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        self.inner.is_needed(token)
    }

    fn is_eligible(
        &self,
        endpoint: &Self::Endpoint,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> bool {
        self.inner.is_eligible(endpoint, token)
            && self
                .chosen
                .values()
                .is_ok_and(|chosen| chosen.len() < self.limit.get().get())
    }

    fn is_retained(
        &self,
        endpoint: &Self::Endpoint,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> bool {
        self.inner.is_retained(endpoint, token)
    }
}

pub struct Renewal<'a> {
    pub(crate) demand: &'a DemandState,
    pub(crate) limit: &'a Cell<NonZero<usize>>,
    pub(crate) step: &'a Cell<u8>,
}

impl<'d> root::Root<'d> for Renewal<'_> {
    type Output = ();

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<()> {
        let (this, wake, _, _) = context.into_parts();
        match this.step.get() {
            0 => {
                this.demand.needed.set(false);
                this.limit.set(NonZero::<usize>::MIN.saturating_add(1));
                this.step.set(1);
                wake.wake();
            }
            1 => {
                this.demand.needed.set(true);
                this.step.set(2);
            }
            _ => {}
        }
        Poll::Pending
    }
}
