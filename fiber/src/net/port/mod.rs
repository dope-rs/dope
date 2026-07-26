pub(in crate::net) mod recv;
pub(crate) mod result;
mod state;

use std::cell::Cell;

use o3::buffer::{RetainBytes, Shared};
use o3::collections::CellQueue;

use crate::Waker;
use dope::driver::token::Token;
use dope::io::provided::ProvidedView;
use std::io::Error;
use std::io::ErrorKind;

use dope::manifold::connector::app::{self, CloseKind};
use recv::arena::{RecvArena, RecvLayout};
use result::{RecvInto, SendIdle};
use state::State;

struct Entry<'d> {
    token: Cell<Option<Token>>,
    state: State<'d>,
    send: Cell<Option<Shared>>,
    send_pending: Cell<bool>,
    close: Cell<bool>,
    wake: Cell<Option<Waker<'d>>>,
    request_queued: Cell<bool>,
    inflight: Cell<bool>,
}

impl Default for Entry<'_> {
    fn default() -> Self {
        Self {
            token: Cell::new(None),
            state: State::default(),
            send: Cell::new(None),
            send_pending: Cell::new(false),
            close: Cell::new(false),
            wake: Cell::new(None),
            request_queued: Cell::new(false),
            inflight: Cell::new(false),
        }
    }
}

pub(crate) struct Requests {
    pub(crate) send: Option<Shared>,
    pub(crate) close: bool,
}

pub struct Port<'d> {
    entries: Box<[Entry<'d>]>,
    recv: RecvArena<'d>,
    deferred_requests: Option<CellQueue<Token>>,
}

impl<'d> Port<'d> {
    pub(in crate::net) fn with_layout(layout: RecvLayout, deferred_requests: bool) -> Self {
        Self::build(layout, deferred_requests)
    }

    fn build(layout: RecvLayout, deferred_requests: bool) -> Self {
        let capacity = layout.connections();
        let recv = RecvArena::with_layout(layout);
        let entries = (0..capacity).map(|_| Entry::default()).collect();
        Self {
            entries,
            recv,
            deferred_requests: deferred_requests.then(|| CellQueue::with_capacity(capacity)),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.entries.len()
    }

    fn entry(&self, token: Token) -> Option<&Entry<'d>> {
        let entry = self.entries.get(token.slot().raw() as usize)?;
        entry
            .token
            .get()
            .is_some_and(|current| current.same_target(token))
            .then_some(entry)
    }

    fn state<'a>(entry: &'a Entry<'d>) -> &'a State<'d> {
        &entry.state
    }

    fn wake(entry: &Entry<'d>) {
        if let Some(wake) = entry.wake.get() {
            wake.wake();
        }
    }

    fn notify_requests(&self, token: Token, entry: &Entry<'d>) {
        let Some(queue) = &self.deferred_requests else {
            Self::wake(entry);
            return;
        };
        if entry.request_queued.replace(true) {
            return;
        }
        assert!(
            queue.push_back(token).is_ok(),
            "fiber: deferred request queue capacity invariant"
        );
    }

    pub(crate) fn activate(&self, token: Token, wake: Waker<'d>) -> bool {
        self.activate_with(token, Some(wake))
    }

    pub(crate) fn activate_deferred(&self, token: Token) -> bool {
        self.activate_with(token, None)
    }

    fn activate_with(&self, token: Token, wake: Option<Waker<'d>>) -> bool {
        let Some(entry) = self.entries.get(token.slot().raw() as usize) else {
            return false;
        };
        if !entry.state.reset(&self.recv) {
            return false;
        }
        entry.send.take();
        entry.send_pending.set(false);
        entry.close.set(false);
        entry.wake.set(wake);
        entry.request_queued.set(false);
        entry.inflight.set(false);
        entry.token.set(Some(token));
        true
    }

    pub(crate) fn contains(&self, token: Token) -> bool {
        self.entry(token).is_some()
    }

    pub(crate) fn push_recv<R: RetainBytes>(&self, token: Token, chunk: R) -> bool {
        self.entry(token)
            .is_none_or(|entry| Self::state(entry).push_recv(&self.recv, chunk))
    }

    pub(crate) fn push_retained(&self, token: Token, chunk: ProvidedView<'d>) -> bool {
        self.entry(token)
            .is_none_or(|entry| Self::state(entry).push_retained(&self.recv, chunk))
    }

    pub(crate) fn closed(&self, token: Token) {
        if let Some(entry) = self.entry(token) {
            Self::state(entry).signal_closed();
        }
    }

    pub(crate) fn failed(&self, token: Token) {
        if let Some(entry) = self.entry(token) {
            Self::state(entry).signal_error(Error::new(
                ErrorKind::OutOfMemory,
                "fiber: egress queue over cap",
            ));
        }
    }

    pub(crate) fn drain_requests(
        &self,
        token: Token,
        mut push: impl FnMut(Shared) -> Result<(), Shared>,
    ) -> Option<app::Requests> {
        let entry = self.entry(token)?;
        if let Some(send) = entry.send.take() {
            entry.send_pending.set(false);
            if let Err(send) = push(send) {
                entry.send.set(Some(send));
                entry.send_pending.set(true);
            }
        }
        Some(app::Requests {
            close: entry.close.take().then_some(CloseKind::Reconnect),
        })
    }

    pub(crate) fn requests(&self, token: Token) -> Option<Requests> {
        let entry = self.entry(token)?;
        entry.request_queued.set(false);
        let send = entry.send.take();
        if send.is_some() {
            entry.send_pending.set(false);
        }
        Some(Requests {
            send,
            close: entry.close.take(),
        })
    }

    pub(crate) fn pop_deferred_request(&self) -> Option<Token> {
        self.deferred_requests.as_ref()?.pop_front()
    }

    pub(crate) fn sync_send(&self, token: Token, inflight: bool) {
        if let Some(entry) = self.entry(token) {
            entry.inflight.set(inflight);
            Self::state(entry).wake_send();
        }
    }

    pub(crate) fn readable_drained(&self, token: Token) -> bool {
        self.entry(token)
            .is_none_or(|entry| Self::state(entry).readable_drained())
    }

    pub(crate) fn recv_into(&self, token: Token, dst: &mut [u8]) -> RecvInto {
        self.entry(token).map_or(RecvInto::Bytes(0), |entry| {
            Self::state(entry).try_recv_into(&self.recv, dst)
        })
    }

    pub(crate) fn recv_waker(&self, token: Token, waker: Waker<'d>) {
        if let Some(entry) = self.entry(token) {
            Self::state(entry).set_recv_waker(waker);
        }
    }

    pub(crate) fn clear_recv_waker(&self, token: Token) {
        if let Some(entry) = self.entry(token) {
            Self::state(entry).clear_recv_waker();
        }
    }

    pub(crate) fn send_waker(&self, token: Token, waker: Waker<'d>) {
        if let Some(entry) = self.entry(token) {
            Self::state(entry).set_send_waker(waker);
        }
    }

    pub(crate) fn clear_send_waker(&self, token: Token) {
        if let Some(entry) = self.entry(token) {
            Self::state(entry).clear_send_waker();
        }
    }

    pub(crate) fn send(&self, token: Token, bytes: Shared) {
        let Some(entry) = self.entry(token) else {
            return;
        };
        if let Some(pending) = entry.send.replace(Some(bytes)) {
            entry.send.set(Some(pending));
            Self::state(entry).signal_error(Error::new(
                ErrorKind::OutOfMemory,
                "fiber: send already pending",
            ));
            return;
        }
        entry.send_pending.set(true);
        entry.inflight.set(true);
        self.notify_requests(token, entry);
    }

    pub(crate) fn send_idle(&self, token: Token) -> SendIdle {
        let Some(entry) = self.entry(token) else {
            return SendIdle::Idle;
        };
        Self::state(entry).send_status(entry.send_pending.get() || entry.inflight.get())
    }

    pub(crate) fn close(&self, token: Token) {
        if let Some(entry) = self.entry(token) {
            Self::state(entry).detach();
            entry.close.set(true);
            self.notify_requests(token, entry);
        }
    }
}
