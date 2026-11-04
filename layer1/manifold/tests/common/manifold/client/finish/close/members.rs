use std::{cell::Cell, convert::Infallible, net::SocketAddr, num::NonZero, rc::Rc, task::Poll};

use dope_core::{
    driver::schedule::clock,
    io::socket::{self, option, option::Set},
};
use dope_manifold::{
    service,
    service::{
        Endpoint, Revision, Snapshot, discover,
        discover::{Action, Reason},
        observe,
    },
};
use dope_net::Transport;
use dope_runtime::executor::root;
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::base::cell::region;

type Peers = Snapshot<Endpoint<u32, SocketAddr>, 1>;

pub struct Source {
    pub(crate) pending: Rc<Cell<Option<Peers>>>,
    pub(crate) failures: Gate,
}

impl discover::Output for Source {
    type Metadata<'d> = ();
    type Error = Infallible;
}

impl<'d> discover::RefreshAt<'d> for Source {
    fn refresh_at(&mut self, _: clock::Moment<'d>, reason: discover::Reason) {
        if let Reason::Failure(_) = reason {
            self.failures.hit();
        }
    }
}

impl<'d> discover::PollAt<'d, u32, SocketAddr, 1> for Source {
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
    ) -> discover::Action<'d, u32, SocketAddr, (), Infallible, 1> {
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

impl<'d> observe::Audit<'d, u32, SocketAddr, Source, 1> for Watch {
    fn resolved(&mut self, _: &Source, set: &Peers, _: &()) {
        let id = set.endpoints().first().map(|entry| *entry.id());
        self.0.record(Notice::Resolved(set.revision(), id));
    }

    fn stale(&mut self, _: &Source, rev: Revision) {
        self.0.record(Notice::Stale(rev));
    }

    fn expired(&mut self, _: &Source, rev: Revision) {
        self.0.record(Notice::Expired(rev));
    }

    fn reconciled(&mut self, _: &Source, rev: Revision, change: service::Change) {
        self.0.record(Notice::Reconciled(rev, change));
    }
}

impl<'d> observe::Failure<'d, u32, SocketAddr, Source, 1> for Watch {
    fn failed(&mut self, _: &Source, error: Infallible) {
        match error {}
    }

    fn rejected(&mut self, _: &Source, error: service::Reconcile) {
        self.0.record(Notice::Rejected(error));
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
