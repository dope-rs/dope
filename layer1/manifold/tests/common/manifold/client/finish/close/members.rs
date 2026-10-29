use std::{cell::Cell, convert::Infallible, net::SocketAddr, rc::Rc, task::Poll};

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
