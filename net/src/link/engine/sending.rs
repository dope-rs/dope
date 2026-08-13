use std::num;

use dope_core::{
    driver::{self, flight, retained, route},
    io::fd::handles,
};

use crate::{
    link,
    link::engine::lifecycle,
    wire::{reclaim, send},
};

pub(in crate::link) struct Sending {
    limit: Option<num::NonZeroU32>,
}

pub(in crate::link) struct Submission<'a, 'd> {
    sending: &'a mut Sending,
    flights: &'a mut super::Flights<'d>,
    lifecycle: &'a mut lifecycle::Lifecycle,
}

impl Sending {
    pub(super) fn new() -> Self {
        Self { limit: None }
    }

    pub(in crate::link) fn is_inflight(&self) -> bool {
        self.limit.is_some()
    }

    pub(in crate::link) fn complete(
        &mut self,
        flights: &mut super::Flights<'_>,
        lifecycle: &mut lifecycle::Lifecycle,
        bytes: u32,
    ) -> Option<send::Sent> {
        use crate::wire::send::Sent;
        if bytes > self.limit?.get() {
            lifecycle.abort();
            self.done(flights);
            return None;
        }
        self.done(flights);
        Some(Sent::new(bytes))
    }

    pub(in crate::link) fn done(&mut self, flights: &mut super::Flights<'_>) {
        self.limit = None;
        if let Some(flight) = flights.send.take() {
            let _ = flight.complete();
        }
    }

    fn submit<'d, Tag: route::Tag>(
        driver: &mut retained::Context<'_, '_, 'd>,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        send: &impl link::raw::Send,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        send.submit_retained(fd, target, slots, driver)
    }

    pub(in crate::link) fn submission<'a, 'd>(
        &'a mut self,
        flights: &'a mut super::Flights<'d>,
        lifecycle: &'a mut lifecycle::Lifecycle,
    ) -> Submission<'a, 'd> {
        Submission {
            sending: self,
            flights,
            lifecycle,
        }
    }
}

impl<'d> Submission<'_, 'd> {
    pub(in crate::link) fn take_pending_graceful(&mut self) -> bool {
        self.lifecycle.take_graceful(self.sending.is_inflight())
    }

    pub(in crate::link) fn submit_prepared<Tag: route::Tag, P: reclaim::Policy>(
        &mut self,
        fd: &handles::Descriptor<'d>,
        slots: &flight::Slots<'d, Tag>,
        driver: &mut retained::Context<'_, '_, 'd>,
        target: route::Target<'d, Tag>,
        prepared: send::Prepared<'_, P>,
    ) -> send::Outcome<P> {
        use crate::wire::send::Payload;
        let (payload, consumed, close_after) = prepared.into_parts();
        if close_after {
            self.lifecycle.set_close_after();
        }
        if self.sending.is_inflight() {
            return send::Outcome::idle(consumed);
        }
        let (flight, limit) = match payload {
            Payload::Empty => return send::Outcome::idle(consumed),
            Payload::Single(buf) if buf.is_empty() => return send::Outcome::idle(consumed),
            Payload::Single(buf) => {
                let Ok(limit) = u32::try_from(buf.len()) else {
                    return send::Outcome::rejected(consumed);
                };
                let Some(limit) = num::NonZeroU32::new(limit) else {
                    return send::Outcome::idle(consumed);
                };
                let Ok(flight) = Sending::submit(driver, slots, fd, target, &buf) else {
                    return send::Outcome::rejected(consumed);
                };
                (flight, limit)
            }
            Payload::Vectored(vectored) if vectored.is_empty() => {
                return send::Outcome::idle(consumed);
            }
            Payload::Vectored(vectored) => {
                let Ok(limit) = u32::try_from(vectored.bytes()) else {
                    return send::Outcome::rejected(consumed);
                };
                let Some(limit) = num::NonZeroU32::new(limit) else {
                    return send::Outcome::idle(consumed);
                };
                let Ok(flight) = Sending::submit(driver, slots, fd, target, &vectored) else {
                    return send::Outcome::rejected(consumed);
                };
                (flight, limit)
            }
        };
        self.sending.limit = Some(limit);
        self.flights.send = Some(flight);
        send::Outcome::submitted(consumed)
    }
}

impl Sending {
    pub(in crate::link) fn cancel_flight<'a, 'd>(
        &mut self,
        flights: &'a mut super::Flights<'d>,
    ) -> Option<&'a mut flight::Flight<'d>> {
        flights.send.as_mut()
    }
}
