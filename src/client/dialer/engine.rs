use crate::runtime::endpoint::control::actions;
use std::{pin, time};

use dope::core::driver::schedule::clock;
use o3::mem::Rejected;

use crate::client::{self, auth, control::Sender, life, protocol};
use crate::conn::id;
use crate::errors;
use crate::proto::datagram;
use crate::runtime::brand;
use crate::runtime::endpoint::{self, control::SendError, owner};
use crate::runtime::mux::{self, router::Admission};
use crate::runtime::routing;

/// The dialer binds its protocol bridge and receive storage to the same endpoint.
type Connection<'pool, 'd, const ID: u8, P, B, A, R> = endpoint::Endpoint<
    'd,
    ID,
    client::Bridge<'d, ID, P, A, B, R>,
    <P as protocol::Protocol<'d>>::Receive,
    endpoint::Idle,
    <A as auth::Mode>::Binding,
    datagram::Sealed<'pool>,
>;

#[pin_project::pin_project]
pub(in crate::client) struct Engine<
    'pool,
    'd,
    const ID: u8,
    P: protocol::Protocol<'d>,
    B: client::Backoff,
    A: auth::Mode,
    C: client::ConfigSource,
    R: routing::PeerRoute,
> where
    A::Binding: owner::Binding<'d, ID>,
{
    #[pin]
    pub(in crate::client) endpoint: Connection<'pool, 'd, ID, P, B, A, R>,
    config: C,
    retained: Option<id::Handle<id::Driver<'d>, ID>>,
}

impl<'pool, 'd, const ID: u8, P, B, A, C, R> Engine<'pool, 'd, ID, P, B, A, C, R>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
    A: auth::Mode,
    A::Binding: owner::Binding<'d, ID>,
    C: client::ConfigSource,
    R: routing::PeerRoute,
{
    pub(in crate::client) fn new(
        endpoint: Connection<'pool, 'd, ID, P, B, A, R>,
        config: C,
    ) -> Self {
        Self {
            endpoint,
            config,
            retained: None,
        }
    }

    /// The retained transport's existing deadline owns capacity wakeup.
    pub(in crate::client) fn next_retry(&self) -> Option<time::Instant> {
        if self
            .retained
            .is_some_and(|handle| self.endpoint.is_waiting(handle))
        {
            return None;
        }
        self.endpoint.handler().lifecycle().next_retry()
    }

    pub(in crate::client) fn try_due(
        mut self: pin::Pin<&mut Self>,
        scope: brand::Token<'_>,
        now: clock::Moment<'d>,
    ) -> Option<bool> {
        let retry = self.as_ref().get_ref().next_retry()?;
        if now.at(retry).apply(|retry, raw| retry > raw) {
            return None;
        }
        let this = self.as_ref().get_ref();
        match this.endpoint.admission() {
            Admission::Retained(handle) => {
                *self.project().retained = Some(handle);
                return None;
            }
            Admission::Available | Admission::Full => {
                *self.as_mut().project().retained = None;
            }
        }
        let due = now.at(self.as_ref()).apply(|core, raw| {
            let endpoint = &core.get_ref().endpoint;
            endpoint
                .handler()
                .lifecycle()
                .next_due(endpoint.domain().identity(), raw)
        })?;
        let due = match due.transpose() {
            Ok(due) => due,
            Err(failure) => {
                let (due, error) = failure.into_parts();
                return self.failed(scope, due, now, error.into());
            }
        };
        enum Failure {
            Fresh(errors::Connect),
            Reconnect(errors::Reconnect),
        }
        let auth = due.auth();
        let config = self
            .as_mut()
            .project()
            .config
            .config(due.scope(scope.reborrow(), now));
        let Some(config) = config else {
            now.at(self.as_mut()).apply(|core, raw| {
                core.project().endpoint.handler_mut().life_mut().defer_due(
                    scope.reborrow(),
                    due,
                    raw,
                );
            });
            return Some(false);
        };
        let again = self
            .as_mut()
            .project()
            .endpoint
            .handler_mut()
            .life_mut()
            .take_again(&due);
        let result = now.at(self.as_mut()).apply(|core, raw| {
            let mut this = core.project();
            match again {
                Some(again) => auth
                    .reconnect(
                        scope.reborrow(),
                        this.endpoint.as_mut(),
                        config,
                        again,
                        due.route(),
                        raw,
                    )
                    .map_err(Failure::Reconnect),
                None => auth
                    .connect(
                        scope.reborrow(),
                        this.endpoint.as_mut(),
                        due.address(),
                        config,
                        due.route(),
                        raw,
                    )
                    .map_err(Failure::Fresh),
            }
        });
        match result {
            Ok(handle) => {
                self.project()
                    .endpoint
                    .handler_mut()
                    .life_mut()
                    .bind_due(due, handle);
                Some(true)
            }
            Err(failure) => {
                let error = match failure {
                    Failure::Fresh(error) => error,
                    Failure::Reconnect(failure) => {
                        let (error, again) = failure.into_parts();
                        if let Some(again) = again {
                            self.as_mut()
                                .project()
                                .endpoint
                                .handler_mut()
                                .life_mut()
                                .put_again(&due, again);
                        }
                        error
                    }
                };
                self.failed(scope, due, now, error)
            }
        }
    }

    fn failed<T>(
        self: pin::Pin<&mut Self>,
        scope: brand::Token<'_>,
        due: life::Due<T, R>,
        now: clock::Moment<'d>,
        error: errors::Connect,
    ) -> Option<bool> {
        let mut this = self.project();
        this.endpoint
            .as_mut()
            .handler_mut()
            .protocol_pin()
            .failed(due.scope(scope.reborrow(), now), &error);
        let life = this.endpoint.handler_mut().life_mut();
        now.at(life)
            .apply(|life, raw| life.defer_due(scope, due, raw));
        Some(false)
    }

    pub(in crate::client) fn drain_events(self: pin::Pin<&mut Self>, budget: usize) -> usize {
        self.project()
            .endpoint
            .handler_mut()
            .drain_events(budget, |_| {})
    }

    pub(in crate::client) fn drain_ready(
        self: pin::Pin<&mut Self>,
        now: clock::Moment<'d>,
        budget: usize,
    ) -> usize {
        now.at(self)
            .apply(|core, raw| core.drain_ready_at(raw, budget))
    }

    fn drain_ready_at(self: pin::Pin<&mut Self>, now: time::Instant, budget: usize) -> usize {
        let (bridge, mut resume) = actions::split_ready(self.project().endpoint, now);
        bridge.drain_events(budget, |handle| match resume(handle) {
            Ok(()) | Err(errors::Access::Closed) => {}
            Err(errors::Access::InvalidHandle) => unreachable!("generation"),
        })
    }

    pub(in crate::client) fn recv_datagram(
        self: pin::Pin<&mut Self>,
        now: clock::Moment<'d>,
    ) -> Result<Option<(client::AttemptId, P::Receive)>, errors::Access> {
        if P::DATAGRAMS == protocol::Datagrams::Push {
            return Ok(None);
        }
        now.at(self).apply(|core, raw| {
            actions::next_app::<ID, _, _, _, _, _, { mux::DATAGRAM_QUEUE }>(
                core.project().endpoint,
                raw,
                |bridge, mut conn| {
                    let Some(slot) = bridge.bind_input(&mut conn)? else {
                        return Ok(None);
                    };
                    // This receive commits readiness to APP below; it does not rearm
                    // the callback queue or change any existing stream/close effects.
                    let data = conn.transport().reborrow::<false>().datagrams().recv();
                    let remaining = bridge.has_input(conn.transport());
                    conn.set_ready(remaining);
                    Ok(data.map(|data| (slot.attempt_id(), data)))
                },
            )
            .unwrap_or(Ok(None))
        })
    }

    pub(in crate::client) fn recv_stream(
        self: pin::Pin<&mut Self>,
        now: clock::Moment<'d>,
    ) -> Result<Option<(client::AttemptId, protocol::Stream<P::Receive>)>, errors::Access> {
        if P::STREAMS == protocol::Streams::Push {
            return Ok(None);
        }
        now.at(self).apply(|core, raw| {
            actions::next_app::<ID, _, _, _, _, _, { mux::STREAM_QUEUE }>(
                core.project().endpoint,
                raw,
                |mut bridge, mut conn| {
                    let Some(slot) = bridge.bind_input(&mut conn)? else {
                        return Ok(None);
                    };
                    let input = if let Some(lease) = conn.transport().events().poll_stream() {
                        let event = lease.bound_event().event();
                        let (input, accept) =
                            bridge.as_mut().receive_stream(conn.transport(), event);
                        conn.transport().events().finish(lease, accept);
                        input
                    } else {
                        None
                    };
                    let remaining = bridge.has_input(conn.transport());
                    conn.set_ready(remaining);
                    Ok(input.map(|input| (slot.attempt_id(), input)))
                },
            )
            .unwrap_or(Ok(None))
        })
    }

    pub(super) fn close_at(mut self: pin::Pin<&mut Self>, now: time::Instant) {
        self.as_mut()
            .project()
            .endpoint
            .handler_mut()
            .life_mut()
            .seal();
        self.restart_at(now);
    }

    pub(super) fn restart_at(mut self: pin::Pin<&mut Self>, now: time::Instant) {
        let capacity = self.as_ref().get_ref().endpoint.handler().lifecycle().len();
        for index in 0..capacity {
            let handle = self
                .as_ref()
                .get_ref()
                .endpoint
                .handler()
                .lifecycle()
                .handle_at(index);
            if let Some(handle) = handle {
                actions::close(self.as_mut().project().endpoint, handle, now);
            }
        }
        self.as_mut().drain_ready_at(now, usize::MAX);
    }

    pub(super) fn try_send_at(
        mut self: pin::Pin<&mut Self>,
        slot: client::SlotId,
        data: Vec<u8>,
        now: time::Instant,
    ) -> Result<(), errors::SendFailure<Vec<u8>>> {
        let Some(handle) = self
            .as_ref()
            .get_ref()
            .endpoint
            .handler()
            .lifecycle()
            .handle(slot)
        else {
            return Err(errors::SendFailure::Closed(data));
        };
        match actions::try_send_datagram(self.as_mut().project().endpoint, handle, data, now) {
            Err(errors::SendFailure::Closed(data)) => {
                actions::close(self.project().endpoint, handle, now);
                Err(errors::SendFailure::Closed(data))
            }
            result => result,
        }
    }

    pub(super) fn try_pool_at(
        mut self: pin::Pin<&mut Self>,
        slot: client::SlotId,
        packet: datagram::Packet<'pool>,
        now: time::Instant,
    ) -> Result<(), errors::SendFailure<datagram::Packet<'pool>>> {
        let Some(handle) = self
            .as_ref()
            .get_ref()
            .endpoint
            .handler()
            .lifecycle()
            .handle(slot)
        else {
            return Err(errors::SendFailure::Closed(packet));
        };
        let sender = match actions::try_sender(self.as_mut().project().endpoint, now) {
            Ok(sender) => Sender::new(sender),
            Err(SendError::Full) => {
                return Err(errors::SendFailure::Full(packet));
            }
            Err(SendError::Closed) => {
                actions::close(self.project().endpoint, handle, now);
                return Err(errors::SendFailure::Closed(packet));
            }
        };
        sender.try_send(slot, packet).map_err(Rejected::discard)
    }
}
