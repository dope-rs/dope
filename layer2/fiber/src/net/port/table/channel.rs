use dope::net::link::{egress, event};
use o3::{base::cell::region, buffer::resident};

use crate::{
    core::{driver::schedule::ready::wake, io::transfer},
    manifold::app::{dispatch::identity, receive},
    net::{
        error::{self, read},
        port::{
            entry, result, state,
            table::{Table, queues},
        },
    },
};

pub(crate) struct Channel<'a, 'd, R, E, I: identity::Identity> {
    pub(super) port: &'a Table<'d, R, E, I>,
}

pub(crate) enum Quiescence {
    Pending,
    Ready,
}

impl<'a, 'd, R, E, I: identity::Identity> Channel<'a, 'd, R, E, I> {
    pub(crate) fn push(
        self,
        token: I,
        chunk: R,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> state::Push<R>
    where
        R: transfer::Length + Unpin,
    {
        if self.port.shutting_down.get() {
            return state::Push::Closed(chunk);
        }
        let Some(entry) = queues::Bound::get(&self.port.entries, token) else {
            return state::Push::Closed(chunk);
        };
        entry
            .state
            .push(entry.index(), &self.port.recv, chunk, region)
    }

    pub(crate) fn end(self, token: I, end: receive::End) {
        if let Some(entry) = queues::Bound::get(&self.port.entries, token) {
            match end {
                receive::End::Read => entry.state.eof(),
                receive::End::Both => entry.state.begin_close(),
            }
        }
    }

    pub(crate) fn closed(self, token: I) {
        if let Some(entry) = queues::Bound::get(&self.port.entries, token) {
            entry.state.close();
        }
    }

    pub(crate) fn fail_edge(self, token: I, error: read::Edge<E>) {
        if let Some(entry) = queues::Bound::get(&self.port.entries, token) {
            entry.state.fail_edge(error);
        }
    }

    pub(crate) fn fail_receive(self, token: I, error: event::ParkError) {
        if let Some(entry) = queues::Bound::get(&self.port.entries, token) {
            entry.state.fail_receive(error);
        }
    }

    pub(crate) fn sync_send(self, token: I, flight: egress::Pending) {
        if let Some(entry) = queues::Bound::get(&self.port.entries, token) {
            entry.requests.sync(flight);
            entry.state.send.wake_once();
        }
    }

    pub(crate) fn close_send(self, token: I, send: resident::Shared<'d>) {
        let Some(entry) = queues::Bound::get(&self.port.entries, token) else {
            return;
        };
        entry.requests.fail_closed(send);
        entry.state.send.wake_once();
    }

    pub(crate) fn retire_send(self, token: I) {
        if let Some(entry) = queues::Bound::get(&self.port.entries, token) {
            entry.requests.retire();
            entry.state.send.wake_once();
        }
    }

    pub(crate) fn is_held(&self, token: I) -> bool {
        queues::Bound::get(&self.port.entries, token).is_some_and(|entry| entry.state.is_held())
    }

    pub(crate) fn quiescence(self, token: I) -> Quiescence {
        let Some(entry) = queues::Bound::get(&self.port.entries, token) else {
            return Quiescence::Ready;
        };
        match (
            self.port.recv.is_empty(entry.index()),
            entry.requests.pending(self.port.shutting_down.get()),
        ) {
            (true, entry::Presence::Absent) => Quiescence::Ready,
            (false, entry::Presence::Absent | entry::Presence::Present)
            | (true, entry::Presence::Present) => {
                entry.drain_waiting.set(true);
                Quiescence::Pending
            }
        }
    }

    pub(crate) fn recv(
        self,
        token: I,
        region: &mut region::Token<region::Identity<'d>>,
        wake: wake::Waker<'d>,
    ) -> result::Recv<R, E>
    where
        R: Unpin,
    {
        use crate::net::port::result::Recv;

        let Some(bound) = queues::Bound::get(&self.port.entries, token) else {
            return Recv::Closed;
        };
        let (result, empty) = bound.state.try_recv(bound.index(), &self.port.recv, region);
        if matches!(result, Recv::Pending) {
            bound.state.recv.set(wake);
        }
        if matches!(result, Recv::Ready(_)) && empty && bound.drain_waiting.take() {
            self.port.deferred.notify(&self.port.entries, &bound);
        }
        result
    }

    pub(crate) fn clear_recv(self, token: I) {
        if let Some(entry) = queues::Bound::get(&self.port.entries, token) {
            entry.state.recv.clear();
        }
    }

    pub(crate) fn clear_send(self, token: I) {
        if let Some(entry) = queues::Bound::get(&self.port.entries, token) {
            entry.state.send.clear();
        }
    }

    pub(crate) fn try_send(
        self,
        token: I,
        bytes: resident::Shared<'d>,
        wake: wake::Waker<'d>,
    ) -> result::StageSend<'d> {
        use crate::net::port::result::StageSend;

        if self.port.shutting_down.get() {
            return StageSend::Failed(error::Write::PortShuttingDown { payload: bytes });
        }
        let Some(bound) = queues::Bound::get(&self.port.entries, token) else {
            return StageSend::Failed(error::Write::ConnectionUnavailable {
                payload: Some(bytes),
            });
        };
        if let Err(cause) = bound.state.start_send() {
            let error = match cause {
                state::SendError::Closed => error::Write::ConnectionClosed {
                    payload: Some(bytes),
                },
                state::SendError::Failed => error::Write::ConnectionFailed {
                    payload: Some(bytes),
                },
            };
            return StageSend::Failed(error);
        }
        match bound.requests.try_stage(bytes) {
            StageSend::Staged => {}
            StageSend::Busy(bytes) => {
                bound.state.send.set(wake);
                return StageSend::Busy(bytes);
            }
            StageSend::Failed(error) => return StageSend::Failed(error),
        }
        bound.state.send.set(wake);
        self.port.deferred.notify(&self.port.entries, &bound);
        StageSend::Staged
    }

    pub(crate) fn cancel_send(self, token: I) {
        let Some(entry) = queues::Bound::get(&self.port.entries, token) else {
            return;
        };
        entry.requests.cancel();
        entry.state.send.clear();
    }

    pub(crate) fn send_status(self, token: I, wake: wake::Waker<'d>) -> result::SendStatus<'d> {
        let Some(entry) = queues::Bound::get(&self.port.entries, token) else {
            use crate::net::port::result::SendStatus;

            return SendStatus::Failed(error::Write::ConnectionUnavailable { payload: None });
        };
        let status = match entry.requests.take_failure() {
            Some(error::Write::ConnectionClosed { payload })
                if matches!(entry.state.start_send(), Err(state::SendError::Failed)) =>
            {
                result::SendStatus::Failed(error::Write::ConnectionFailed { payload })
            }
            Some(error) => result::SendStatus::Failed(error),
            None => entry.state.send_status(entry.requests.presence()),
        };
        match status {
            result::SendStatus::Pending => entry.state.send.set(wake),
            result::SendStatus::Complete | result::SendStatus::Failed(_) => entry.requests.cancel(),
        }
        status
    }

    pub(crate) fn close(self, token: I) {
        let Some(bound) = queues::Bound::get(&self.port.entries, token) else {
            return;
        };
        let first = bound.state.detach();
        bound.requests.close.set(true);
        if first == state::Detach::Repeated {
            return;
        }
        if !self.port.recv.is_empty(bound.index()) {
            self.port
                .cleanup
                .push(&self.port.entries, &bound, |entry| &entry.cleanup);
        }
        self.port.deferred.notify(&self.port.entries, &bound);
    }
}
