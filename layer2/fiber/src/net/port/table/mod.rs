use std::cell;

use dope::core::driver::route::Identity;
use o3::{alloc, base::cell::region, buffer::resident, mem::quota};

use crate::{
    context::wake,
    core::driver::{
        route::table::conn,
        schedule::{self, app},
    },
    manifold::{
        app::{dispatch::identity, receive::limits},
        client::app::request::Requests,
    },
    net::port::{self, active, entry, request, state},
};

pub(crate) mod channel;
pub(super) mod deferred;
pub(super) mod queues;
pub(super) mod raw;

pub(crate) struct Table<'d, R, E, I: identity::Identity> {
    entries: raw::Entries<'d, E, I>,
    pub(super) recv: port::Queue<'d, R>,
    deferred: deferred::Deferred<I>,
    cleanup: queues::Fifo<I, queues::Cleanup>,
    pub(super) shutting_down: cell::Cell<bool>,
}

impl<'d, R, E, I: identity::Identity> Table<'d, R, E, I> {
    pub(in crate::net) fn try_new(
        layout: port::Plan,
        defer: bool,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error> {
        let capacity = layout.connections();
        let recv = port::Queue::try_new(layout, token)?;
        let entries = raw::Entries::try_new(capacity)?;
        Ok(Self {
            entries,
            recv,
            deferred: deferred::Deferred::new(defer),
            cleanup: queues::Fifo::new(),
            shutting_down: cell::Cell::new(false),
        })
    }

    pub(crate) const fn capacity(&self) -> conn::Capacity {
        self.entries.capacity()
    }

    pub(in crate::net) fn retention(&self) -> limits::Retention {
        self.recv.retention()
    }

    pub(crate) fn channel(&self) -> channel::Channel<'_, 'd, R, E, I> {
        channel::Channel { port: self }
    }

    pub(crate) fn activate(&self, token: I, wake: wake::RootWaker<'d>) -> active::Activation {
        self.activate_as(token, Some(wake))
    }

    pub(crate) fn defer(&self, token: I) -> active::Activation {
        self.activate_as(token, None)
    }

    fn activate_as(&self, token: I, wake: Option<wake::RootWaker<'d>>) -> active::Activation {
        let Some(entry) = self.entries.get(token.slot()) else {
            return active::Activation::Rejected;
        };
        if self.shutting_down.get()
            || !entry.cleanup.is_vacant()
            || !entry.deferred.is_vacant()
            || !self.recv.is_empty(token.slot())
        {
            return active::Activation::Rejected;
        }
        match entry.requests.presence() {
            entry::Presence::Present => return active::Activation::Rejected,
            entry::Presence::Absent => {}
        }
        if state::Change::Retained == entry.state.activate() {
            return active::Activation::Rejected;
        }
        entry.root_wake.set(wake);
        entry.requests.close.set(false);
        entry.drain_waiting.set(false);
        entry.token.set(Some(token));
        active::Activation::Active
    }

    pub(crate) fn drain<ROUTE: Identity>(
        &self,
        token: I,
        requests: &mut Requests<'_, '_, '_, 'd, resident::Shared<'d>, ROUTE>,
    ) {
        use crate::manifold::client::close::{Directive, LocalOrigin, Reason};

        let Some(entry) = queues::Bound::get(&self.entries, token) else {
            return;
        };
        if let Some(staged) = entry.requests.front(&entry.state.send)
            && let Err(reason) = staged.try_admit(requests)
        {
            requests.close(Directive::cut_retry(Reason::Egress(reason)));
        }
        if entry.requests.close.take() {
            requests.close(Directive::reconnect(Reason::Local(LocalOrigin::ApiRequest)));
        }
    }

    fn take<'a>(entry: &'a entry::Entry<'d, E, I>) -> request::Requests<'a, 'd> {
        let send = entry.requests.front(&entry.state.send);
        request::Requests {
            send,
            close: entry.requests.close.take(),
        }
    }

    pub(crate) fn requests(&self, token: I) -> Option<request::Requests<'_, 'd>> {
        queues::Bound::get(&self.entries, token).map(|bound| {
            let (_, entry) = bound.into_parts();
            Self::take(entry)
        })
    }

    pub(in crate::net) fn pop_deferred(&self) -> Option<request::Deferred<'_, 'd, I>> {
        let bound = self.deferred.pop(&self.entries)?;
        let (token, entry) = bound.into_parts();
        Some(request::Deferred {
            token,
            requests: Self::take(entry),
        })
    }

    fn cleanup(
        &self,
        front: queues::Front<'_, 'd, E, I, queues::Cleanup>,
        region: &mut region::Token<region::Identity<'d>>,
    ) where
        R: Unpin,
    {
        let empty = self.recv.pop_state(front.index(), region).1;
        if empty {
            let bound = front.pop();
            bound.drain_waiting.set(false);
            if !self.shutting_down.get() {
                self.deferred.notify(&self.entries, &bound);
            }
        }
    }

    fn shutdown_at(
        &self,
        entry: raw::Shutdown<'_, 'd, E, I>,
        region: &mut region::Token<region::Identity<'d>>,
    ) where
        R: Unpin,
    {
        let slot = entry.index();
        if !entry.cleanup.is_vacant() {
            entry.advance();
            return;
        }
        let (removed, empty) = {
            let (chunk, empty) = self.recv.pop_state(slot, region);
            (chunk.is_some(), empty)
        };
        if removed {
            match (empty, entry.requests.presence()) {
                (true, entry::Presence::Absent) => entry.advance(),
                (false, entry::Presence::Absent | entry::Presence::Present)
                | (true, entry::Presence::Present) => {}
            }
            return;
        }
        entry.requests.close();
        entry.advance();
    }

    pub(crate) fn pre_park(
        &self,
        work: app::Work<'_, 'd>,
        region: &mut region::Token<region::Identity<'d>>,
    ) where
        R: Unpin,
    {
        loop {
            match work.run(|| {
                if let Some(front) = self.cleanup.front(&self.entries, |entry| &entry.cleanup) {
                    self.cleanup(front, region);
                    return Some(());
                }
                let entry = self.entries.shutdown()?;
                self.shutdown_at(entry, region);
                Some(())
            }) {
                quota::Admission::Item(()) => {}
                quota::Admission::Empty | quota::Admission::Exhausted => return,
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        if self.shutting_down.replace(true) {
            return;
        }
        self.entries.close();
    }

    pub(crate) fn progress(&self) -> schedule::Progress<'d> {
        if self.deferred.is_ready() || self.cleanup.is_ready() || self.entries.is_closing() {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }
}
