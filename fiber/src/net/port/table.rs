use std::{cell, io, process};

use dope::{
    core::driver::schedule::{self, ready::completion},
    manifold::{connector::app, dispatch::typed::identity},
    net::{link::egress::data, wire},
};
use o3::{cell::region, collections, queue};

use crate::{
    context,
    net::port::{entry, recv::arena, result, state},
};

pub(crate) struct Requests<'d> {
    pub(crate) send: Option<data::Buffer<'d>>,
    pub(crate) close: bool,
}

pub(in crate::net) struct DeferredRequest<'d, I: identity::Identity> {
    pub(in crate::net) token: I,
    pub(in crate::net) requests: Requests<'d>,
}

pub(crate) struct Table<'d, R: 'd, I: identity::Identity> {
    entries: Box<[entry::Entry<'d, I>]>,
    recv: arena::RecvArena<'d, R>,
    deferred_requests: Option<queue::Fifo<usize>>,
    cleanup_head: cell::Cell<Option<usize>>,
    cleanup_tail: cell::Cell<Option<usize>>,
    shutting_down: cell::Cell<bool>,
    shutdown_cursor: cell::Cell<Option<usize>>,
}

pub(crate) struct Channel<'a, 'd, R: 'd, I: identity::Identity> {
    port: &'a Table<'d, R, I>,
}

pub(crate) struct Maintenance<'a, 'd, R: 'd, I: identity::Identity> {
    port: &'a Table<'d, R, I>,
}

impl<'a, 'd, R: 'd, I: identity::Identity> Copy for Maintenance<'a, 'd, R, I> {}

impl<'a, 'd, R: 'd, I: identity::Identity> Clone for Maintenance<'a, 'd, R, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, R: wire::Cursor<'d> + 'd, I: identity::Identity> Table<'d, R, I> {
    pub(in crate::net) fn try_with_layout(
        layout: arena::RecvLayout,
        deferred_requests: bool,
    ) -> io::Result<Self> {
        use crate::net::port::recv::arena::RecvArena;
        let capacity = layout.connections();
        let recv = RecvArena::try_with_layout(layout)?;
        let entries =
            collections::BoxSliceExt::try_box_with(capacity, |_| entry::Entry::default())?;
        let deferred_requests = match deferred_requests {
            true => Some(queue::Fifo::try_with_capacity(capacity)?),
            false => None,
        };
        Ok(Self {
            entries,
            recv,
            deferred_requests,
            cleanup_head: cell::Cell::new(None),
            cleanup_tail: cell::Cell::new(None),
            shutting_down: cell::Cell::new(false),
            shutdown_cursor: cell::Cell::new(None),
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn channel(&self) -> super::Channel<'_, 'd, R, I> {
        super::Channel { port: self }
    }

    pub(crate) fn maintenance(&self) -> super::Maintenance<'_, 'd, R, I> {
        super::Maintenance { port: self }
    }

    fn entry(&self, token: I) -> Option<&entry::Entry<'d, I>> {
        let entry = self.entries.get(token.index())?;
        entry.token.matches(token).then_some(entry)
    }

    fn state<'a>(entry: &'a entry::Entry<'d, I>) -> &'a state::State<'d> {
        &entry.state
    }

    fn wake(entry: &entry::Entry<'d, I>) {
        if let Some(wake) = entry.root_wake.get() {
            wake.wake();
        }
    }

    fn notify_requests(&self, token: I, entry: &entry::Entry<'d, I>) {
        let Some(queue) = &self.deferred_requests else {
            Self::wake(entry);
            return;
        };
        if entry.requests.queued.get() {
            return;
        }
        assert!(
            queue.push_back(token.index()).is_ok(),
            "fiber: deferred request queue invariant violated"
        );
        entry.requests.queued.set(true);
    }

    pub(crate) fn activate(
        &self,
        token: I,
        wake: context::RootWaker<'d>,
        region: &mut region::Token<'d>,
    ) -> bool {
        self.activate_with(token, Some(wake), region)
    }

    pub(crate) fn activate_deferred(&self, token: I, region: &mut region::Token<'d>) -> bool {
        self.activate_with(token, None, region)
    }

    fn activate_with(
        &self,
        token: I,
        wake: Option<context::RootWaker<'d>>,
        region: &mut region::Token<'d>,
    ) -> bool {
        let Some(entry) = self.entries.get(token.index()) else {
            return false;
        };
        if self.shutting_down.get()
            || entry.drain.next.get() != entry::CLEANUP_UNLINKED
            || entry.requests.queued.get()
            || entry.requests.pending()
            || !self
                .recv
                .lane_is_empty(token.index(), entry.state.queue(), region)
        {
            return false;
        }
        entry.state.activate_empty();
        entry.root_wake.set(wake);
        entry.requests.inflight.set(false);
        entry.requests.close.set(false);
        entry.drain.next.set(entry::CLEANUP_UNLINKED);
        entry.drain.waiting.set(false);
        let _ = entry.token.replace(token);
        true
    }

    pub(crate) fn drain_requests(
        &self,
        token: I,
        region: &mut region::Token<'d>,
        drain: &mut app::RequestDrain<'_, 'd, data::Buffer<'d>>,
    ) -> Option<app::Requests> {
        use dope::manifold::connector::app::CloseKind;
        let entry = self.entry(token)?;
        if let app::RequestAdmission::Item(send, permit) = entry.requests.admit(drain) {
            match permit.try_push(region, send) {
                Ok(()) => entry.requests.inflight.set(true),
                Err(send) => {
                    entry.requests.restore(send);
                }
            }
        }
        Some(app::Requests {
            close: entry.requests.close.take().then_some(CloseKind::Reconnect),
        })
    }

    fn take_requests(entry: &entry::Entry<'d, I>) -> Requests<'d> {
        let send = entry.requests.take();
        Requests {
            send,
            close: entry.requests.close.take(),
        }
    }

    pub(crate) fn requests(&self, token: I) -> Option<Requests<'d>> {
        self.entry(token).map(Self::take_requests)
    }

    pub(in crate::net) fn pop_deferred_request(&self) -> Option<DeferredRequest<'d, I>> {
        let slot = self.deferred_requests.as_ref()?.pop_front()?;
        let entry = self.entries.get(slot)?;
        entry.requests.queued.set(false);
        Some(DeferredRequest {
            token: entry.token.current()?,
            requests: Self::take_requests(entry),
        })
    }

    pub(crate) fn has_deferred_requests(&self) -> bool {
        self.deferred_requests
            .as_ref()
            .is_some_and(|queue| !queue.is_empty())
    }
}

impl<'d, R: wire::Cursor<'d> + 'd, I: identity::Identity> Maintenance<'_, 'd, R, I> {
    fn enqueue_cleanup(self, token: I, entry: &entry::Entry<'d, I>) {
        if entry.drain.next.get() != entry::CLEANUP_UNLINKED {
            return;
        }
        let port = self.port;
        let index = token.index();
        entry.drain.next.set(entry::CLEANUP_TAIL);
        match port.cleanup_tail.replace(Some(index)) {
            Some(tail) => port.entries[tail].drain.next.set(index as u32),
            None => port.cleanup_head.set(Some(index)),
        }
    }

    fn finish_cleanup(self, index: usize) {
        let port = self.port;
        let Some(head) = port.cleanup_head.get() else {
            process::abort();
        };
        if head != index {
            process::abort();
        }
        let entry = &port.entries[index];
        let link = entry.drain.next.replace(entry::CLEANUP_UNLINKED);
        let next = match link {
            entry::CLEANUP_TAIL => None,
            entry::CLEANUP_UNLINKED => process::abort(),
            next => Some(next as usize),
        };
        port.cleanup_head.set(next);
        if next.is_none() {
            port.cleanup_tail.set(None);
        }
        entry.drain.waiting.set(false);
        if !port.shutting_down.get()
            && let Some(token) = entry.token.current()
        {
            port.notify_requests(token, entry);
        }
    }

    fn cleanup_step(self, index: usize, region: &mut region::Token<'d>) {
        let port = self.port;
        let entry = &port.entries[index];
        let value = port.recv.take_front(index, entry.state.queue(), region);
        drop(value);
        if port.recv.lane_is_empty(index, entry.state.queue(), region) {
            self.finish_cleanup(index);
        }
    }

    fn shutdown_step(self, index: usize, region: &mut region::Token<'d>) {
        let port = self.port;
        let entry = &port.entries[index];
        if entry.drain.next.get() != entry::CLEANUP_UNLINKED {
            self.advance_shutdown(index);
            return;
        }
        if !port.recv.lane_is_empty(index, entry.state.queue(), region) {
            let Some(value) = port.recv.take_front(index, entry.state.queue(), region) else {
                process::abort();
            };
            drop(value);
            if port.recv.lane_is_empty(index, entry.state.queue(), region)
                && !entry.requests.pending()
            {
                self.advance_shutdown(index);
            }
            return;
        }
        if let Some(send) = entry.requests.take() {
            drop(send);
            self.advance_shutdown(index);
            return;
        }
        self.advance_shutdown(index);
    }

    fn advance_shutdown(self, index: usize) {
        let next = index + 1;
        self.port
            .shutdown_cursor
            .set((next < self.port.entries.len()).then_some(next));
    }

    pub(crate) fn pre_park(
        self,
        work: schedule::Application<'_, 'd>,
        region: &mut region::Token<'d>,
    ) {
        loop {
            if let Some(index) = self.port.cleanup_head.get() {
                if !work.take() {
                    return;
                }
                self.cleanup_step(index, region);
                continue;
            }
            let Some(index) = self.port.shutdown_cursor.get() else {
                return;
            };
            if !work.take() {
                return;
            }
            self.shutdown_step(index, region);
        }
    }

    pub(crate) fn begin_shutdown(self) {
        if self.port.shutting_down.replace(true) {
            return;
        }
        self.port
            .shutdown_cursor
            .set((!self.port.entries.is_empty()).then_some(0));
    }

    pub(crate) fn progress(self) -> schedule::Progress<'d> {
        if self.port.cleanup_head.get().is_some() || self.port.shutdown_cursor.get().is_some() {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }
}

impl<'a, 'd, R: wire::Cursor<'d> + 'd, I: identity::Identity> Channel<'a, 'd, R, I> {
    pub(crate) fn push_retained(self, token: I, chunk: R, region: &mut region::Token<'d>) -> bool {
        if self.port.shutting_down.get() {
            return true;
        }
        self.port.entry(token).is_none_or(|entry| {
            Table::<R, I>::state(entry).push_retained(token.index(), &self.port.recv, chunk, region)
        })
    }

    pub(crate) fn closed(self, token: I) {
        if let Some(entry) = self.port.entry(token) {
            Table::<R, I>::state(entry).signal_closed();
        }
    }

    pub(crate) fn out_of_memory(self, token: I) {
        if let Some(entry) = self.port.entry(token) {
            Table::<R, I>::state(entry).signal_out_of_memory();
        }
    }

    pub(crate) fn sync_send(self, token: I, inflight: bool) {
        if let Some(entry) = self.port.entry(token) {
            entry.requests.inflight.set(inflight);
            Table::<R, I>::state(entry).waiters.wake_send();
        }
    }

    pub(crate) fn retained_drained(self, token: I) -> bool {
        self.port.entry(token).is_none_or(|entry| {
            let drained =
                Table::<R, I>::state(entry).readable_drained() && !entry.requests.pending();
            if !drained {
                entry.drain.waiting.set(true);
            }
            drained
        })
    }

    pub(crate) fn recv(self, token: I, region: &mut region::Token<'d>) -> result::Recv<R> {
        use crate::net::port::result::Recv;

        self.port.entry(token).map_or(Recv::Closed, |entry| {
            let result =
                Table::<R, I>::state(entry).try_recv(token.index(), &self.port.recv, region);
            if matches!(result, Recv::Ready(_))
                && Table::<R, I>::state(entry).readable_drained()
                && entry.drain.waiting.take()
            {
                self.port.notify_requests(token, entry);
            }
            result
        })
    }

    pub(crate) fn recv_waker(self, token: I, wake: completion::Waker<'d>) {
        if let Some(entry) = self.port.entry(token) {
            Table::<R, I>::state(entry).waiters.set_recv(wake);
        }
    }

    pub(crate) fn clear_recv_waker(self, token: I) {
        if let Some(entry) = self.port.entry(token) {
            Table::<R, I>::state(entry).waiters.clear_recv();
        }
    }

    pub(crate) fn send_waker(self, token: I, wake: completion::Waker<'d>) {
        if let Some(entry) = self.port.entry(token) {
            Table::<R, I>::state(entry).waiters.set_send(wake);
        }
    }

    pub(crate) fn clear_send_waker(self, token: I) {
        if let Some(entry) = self.port.entry(token) {
            Table::<R, I>::state(entry).waiters.clear_send();
        }
    }

    pub(crate) fn try_stage_send(self, token: I, bytes: data::Buffer<'d>) -> result::StageSend<'d> {
        use crate::net::port::result::StageSend;

        if self.port.shutting_down.get() {
            return StageSend::Failed(io::ErrorKind::BrokenPipe);
        }
        let Some(entry) = self.port.entry(token) else {
            return StageSend::Failed(io::ErrorKind::BrokenPipe);
        };
        if let Err(error) = Table::<R, I>::state(entry).start_send() {
            return StageSend::Failed(error);
        }
        if let Err(bytes) = entry.requests.try_stage(bytes) {
            return StageSend::Busy(bytes);
        }
        self.port.notify_requests(token, entry);
        StageSend::Staged
    }

    pub(crate) fn cancel_staged_send(self, token: I) {
        let Some(entry) = self.port.entry(token) else {
            return;
        };
        let _ = entry.requests.take();
    }

    pub(crate) fn send_status(self, token: I) -> result::SendStatus {
        let Some(entry) = self.port.entry(token) else {
            use crate::net::port::result::SendStatus;

            return SendStatus::Failed(io::ErrorKind::BrokenPipe);
        };
        Table::<R, I>::state(entry)
            .send_status(entry.requests.pending() || entry.requests.inflight.get())
    }

    pub(crate) fn close(self, token: I) {
        if let Some(entry) = self.port.entry(token) {
            let first = Table::<R, I>::state(entry).detach();
            entry.requests.close.set(true);
            if !first {
                return;
            }
            if !Table::<R, I>::state(entry).readable_drained() {
                self.port.maintenance().enqueue_cleanup(token, entry);
            }
            self.port.notify_requests(token, entry);
        }
    }
}
