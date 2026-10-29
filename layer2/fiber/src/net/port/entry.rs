use std::cell;

use dope::{
    core::driver::{route::Identity, schedule::ready::wake::store},
    net::link::{
        egress::{self, reserve::exact},
        pool::send,
    },
};
use o3::buffer::resident;

use crate::{
    context::wake,
    manifold::{app::dispatch::identity, client::app::request},
    net::{
        error,
        port::{result, state, table::queues},
    },
};

enum Request<'d> {
    Idle,
    Staged(egress::NonEmpty<resident::Shared<'d>>),
    Inflight,
    /// Transport remains busy without a write consumer, including after cancellation.
    Cancelled,
    Queued(egress::NonEmpty<resident::Shared<'d>>),
    Failed(error::Write<'d>),
}

pub(super) struct RequestState<'d> {
    state: cell::Cell<Request<'d>>,
    pub(super) close: cell::Cell<bool>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum Presence {
    Absent,
    Present,
}

#[must_use = "the detached send must be committed, restored, or cancelled"]
pub(crate) struct Staged<'a, 'd> {
    state: &'a RequestState<'d>,
    wake: &'a store::Slot<'d>,
    pending: egress::NonEmpty<resident::Shared<'d>>,
    flight: egress::Pending,
}

pub(super) struct Entry<'d, E, I: identity::Identity> {
    pub(super) token: cell::Cell<Option<I>>,
    pub(super) state: state::State<'d, E>,
    pub(super) root_wake: cell::Cell<Option<wake::RootWaker<'d>>>,
    pub(super) requests: RequestState<'d>,
    pub(super) deferred: queues::Link<I, queues::Deferred>,
    pub(super) drain_waiting: cell::Cell<bool>,
    pub(super) cleanup: queues::Link<I, queues::Cleanup>,
}

impl<'d> RequestState<'d> {
    pub(super) fn presence(&self) -> Presence {
        self.pending(false)
    }

    /// Shutdown retains terminal results without waiting for their consumers.
    pub(super) fn pending(&self, shutdown: bool) -> Presence {
        let state = self.state.replace(Request::Idle);
        let presence = match state {
            Request::Idle => Presence::Absent,
            Request::Failed(_) if shutdown => Presence::Absent,
            Request::Staged(_)
            | Request::Inflight
            | Request::Cancelled
            | Request::Queued(_)
            | Request::Failed(_) => Presence::Present,
        };
        self.state.set(state);
        presence
    }

    pub(super) fn sync(&self, flight: egress::Pending) {
        let next = match (self.state.replace(Request::Idle), flight) {
            (Request::Idle | Request::Inflight | Request::Cancelled, egress::Pending::Absent) => {
                Request::Idle
            }
            (Request::Inflight, egress::Pending::Present) => Request::Inflight,
            (Request::Idle | Request::Cancelled, egress::Pending::Present) => Request::Cancelled,
            (Request::Staged(pending) | Request::Queued(pending), egress::Pending::Absent) => {
                Request::Staged(pending)
            }
            (Request::Staged(pending) | Request::Queued(pending), egress::Pending::Present) => {
                Request::Queued(pending)
            }
            (Request::Failed(failure), egress::Pending::Absent | egress::Pending::Present) => {
                Request::Failed(failure)
            }
        };
        self.state.set(next);
    }

    pub(super) fn front<'a>(&'a self, wake: &'a store::Slot<'d>) -> Option<Staged<'a, 'd>> {
        let (pending, flight) = match self.state.replace(Request::Idle) {
            Request::Staged(pending) => (pending, egress::Pending::Absent),
            Request::Queued(pending) => (pending, egress::Pending::Present),
            state => {
                self.state.set(state);
                return None;
            }
        };
        Some(Staged {
            state: self,
            wake,
            pending,
            flight,
        })
    }

    pub(super) fn try_stage(&self, send: resident::Shared<'d>) -> result::StageSend<'d> {
        use result::StageSend;
        match self.state.replace(Request::Idle) {
            Request::Idle => {
                let send = match egress::Load::new(send).try_nonempty() {
                    Ok(send) => send,
                    Err(rejected) => return StageSend::Failed(error::Write::Invalid(rejected)),
                };
                self.state.set(Request::Staged(send));
                StageSend::Staged
            }
            Request::Failed(failure) => StageSend::Failed(failure),
            state => {
                self.state.set(state);
                StageSend::Busy(send)
            }
        }
    }

    pub(super) fn take_failure(&self) -> Option<error::Write<'d>> {
        match self.state.replace(Request::Idle) {
            Request::Failed(failure) => Some(failure),
            state => {
                self.state.set(state);
                None
            }
        }
    }

    pub(super) fn close(&self) {
        let next = match self.state.replace(Request::Idle) {
            Request::Staged(pending) | Request::Queued(pending) => {
                Request::Failed(error::Write::ConnectionClosed {
                    payload: Some(pending.into_inner()),
                })
            }
            state => state,
        };
        self.state.set(next);
    }

    /// Settles requests after all transport send ownership has returned.
    pub(super) fn retire(&self) {
        let state = match self.state.replace(Request::Idle) {
            Request::Cancelled => Request::Idle,
            Request::Inflight => Request::Failed(error::Write::ConnectionClosed { payload: None }),
            Request::Staged(pending) | Request::Queued(pending) => {
                Request::Failed(error::Write::ConnectionClosed {
                    payload: Some(pending.into_inner()),
                })
            }
            state => state,
        };
        self.state.set(state);
    }

    pub(super) fn fail_closed(&self, send: resident::Shared<'d>) {
        let failure = match self.state.replace(Request::Idle) {
            Request::Cancelled => return,
            Request::Idle | Request::Inflight => error::Write::ConnectionClosed {
                payload: Some(send),
            },
            Request::Staged(pending) | Request::Queued(pending) => error::Write::ConnectionClosed {
                payload: Some(pending.into_inner()),
            },
            Request::Failed(failure) => failure,
        };
        self.state.set(Request::Failed(failure));
    }

    pub(super) fn cancel(&self) {
        let next = match self.state.replace(Request::Idle) {
            Request::Queued(_) | Request::Inflight | Request::Cancelled => Request::Cancelled,
            Request::Idle | Request::Staged(_) | Request::Failed(_) => Request::Idle,
        };
        self.state.set(next);
    }
}

impl<'d> Staged<'_, 'd> {
    pub(super) fn try_admit<ROUTE: Identity>(
        mut self,
        requests: &mut request::Requests<'_, '_, '_, 'd, resident::Shared<'d>, ROUTE>,
    ) -> Result<(), exact::Failure> {
        match requests.admit(self.pending) {
            request::Admission::Ready(ready) => {
                self.state.state.set(Request::Inflight);
                ready.push();
                Ok(())
            }
            request::Admission::Exhausted(send) | request::Admission::Saturated(send) => {
                self.pending = send;
                self.restore();
                Ok(())
            }
            request::Admission::Rejected(rejected) => {
                let reason = rejected.cause();
                self.state
                    .state
                    .set(Request::Failed(error::Write::Admission(rejected)));
                self.wake.wake_once();
                Err(reason)
            }
        }
    }

    pub(crate) fn submit(
        self,
        submit: impl FnOnce(
            egress::NonEmpty<resident::Shared<'d>>,
        ) -> Result<(), send::Rejected<resident::Shared<'d>>>,
    ) {
        self.state.state.set(Request::Inflight);
        if let Err(rejected) = submit(self.pending) {
            self.state
                .state
                .set(Request::Failed(error::Write::Rejected(rejected)));
            self.wake.wake_once();
        }
    }

    fn restore(self) {
        let state = match self.flight {
            egress::Pending::Absent => Request::Staged(self.pending),
            egress::Pending::Present => Request::Queued(self.pending),
        };
        self.state.state.set(state);
    }
}

const _: () = {
    assert!(std::mem::size_of::<Presence>() == std::mem::size_of::<bool>());
    assert!(std::mem::align_of::<Presence>() == std::mem::align_of::<bool>());
};

impl<E, I: identity::Identity> Default for Entry<'_, E, I> {
    fn default() -> Self {
        use std::cell::Cell;

        use crate::net::port::state::State;
        Self {
            token: Cell::new(None),
            state: State::default(),
            root_wake: Cell::new(None),
            requests: RequestState {
                state: Cell::new(Request::Idle),
                close: Cell::new(false),
            },
            deferred: queues::Link::new(),
            drain_waiting: Cell::new(false),
            cleanup: queues::Link::new(),
        }
    }
}
