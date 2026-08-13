use std::{num, ops};

use dope_core::driver::{
    flight, retained, route,
    schedule::{self, ready, reservation},
};
use dope_net::{
    link::{egress::data, slot::types},
    wire,
};
use o3::buffer::{self, storage};
use writer::flow::SlotFlow as _;

use crate::listener::{connection, writer};

/// One bounded listener write transaction.
///
/// It carries the connection slot, exact driver flight domain, and one
/// reserved backing buffer together through commit.
///
/// ```compile_fail
/// use dope_manifold::listener::connection::{Ctx, Write};
/// use dope_net::wire::Identity;
///
/// fn escape<'a, 'd, C>(
///     connection: &'a mut Ctx<'a, 'd, 0, Identity, C>,
/// ) -> Write<'d, 'd, 0, Identity, C>
/// where
///     'd: 'a,
/// {
///     connection.try_write().unwrap()
/// }
/// ```
pub struct Write<'a, 'd, const ID: u8, W: wire::Wire, C> {
    slot: &'a mut types::Connection<'d, ID, W, connection::State<'d, ID, C>>,
    flights: &'a flight::Slots<'d, route::KeyTag<ID>>,
    buffer: writer::Buffer<'a, 'd, ID>,
    work: schedule::Application<'a, 'd>,
}

impl<'a, 'd, const ID: u8, W: wire::Wire, C> Write<'a, 'd, ID, W, C> {
    pub(in crate::listener) const fn new(
        slot: &'a mut types::Connection<'d, ID, W, connection::State<'d, ID, C>>,
        flights: &'a flight::Slots<'d, route::KeyTag<ID>>,
        buffer: writer::Buffer<'a, 'd, ID>,
        work: schedule::Application<'a, 'd>,
    ) -> Self {
        Self {
            slot,
            flights,
            buffer,
            work,
        }
    }

    pub fn state(&self) -> &C {
        &self.slot.state.conn
    }

    pub fn state_mut(&mut self) -> &mut C {
        &mut self.slot.state.conn
    }

    pub fn close_after(&self) -> bool {
        self.slot.close_after()
    }

    pub fn wake_target(&self) -> ready::Target<'d> {
        self.slot.io().wake_target()
    }

    /// Borrows application state and the response buffer as disjoint fields.
    pub fn parts_mut(&mut self) -> (&mut C, &mut [u8]) {
        (&mut self.slot.state.conn, self.buffer.as_mut_slice())
    }

    pub fn set_close_after(&mut self) {
        self.slot.set_close_after();
    }

    pub fn submit(self, written: usize, driver: &mut retained::Context<'_, '_, 'd>) -> bool {
        self.submit_parts(written, None, driver)
    }

    pub fn submit_borrowed(
        self,
        written: usize,
        body: &'d [u8],
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        self.submit_body(written, data::Buffer::Borrowed(body), driver)
    }

    pub fn submit_shared(
        self,
        written: usize,
        body: storage::Shared,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        self.submit_body(written, data::Buffer::Shared(body), driver)
    }

    pub fn submit_frozen(
        self,
        written: usize,
        body: buffer::Frozen,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        self.submit_body(written, data::Buffer::Frozen(body), driver)
    }

    fn submit_body(
        self,
        written: usize,
        body: data::Buffer<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        let body = (!body.as_ref().is_empty()).then_some(body);
        self.submit_parts(written, body, driver)
    }

    fn submit_parts(
        self,
        written: usize,
        body: Option<data::Buffer<'d>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        let Self {
            slot,
            flights,
            buffer,
            work,
        } = self;
        match buffer {
            writer::Buffer::Queued {
                slot: header_slot,
                queue,
            } => {
                let Some(written) = slot.accept_write(header_slot.get().as_slice().len(), written)
                else {
                    return false;
                };
                let header = num::NonZeroUsize::new(written.get());
                let entries = usize::from(header.is_some()) + usize::from(body.is_some());
                if entries == 0 {
                    return true;
                }
                let Some(reservation) = reservation::Application::reserve(work, driver, entries)
                else {
                    slot.set_close_after();
                    return false;
                };
                let token = reservation.commit(entries).region_token();
                let header = header.map(|len| {
                    let slot = header_slot.commit();
                    writer::Payload::Header(writer::Header::new(slot, len))
                });
                let body = body.map(writer::Payload::Body);
                let (first, second) = match (header, body) {
                    (Some(header), body) => (header, body),
                    (None, Some(body)) => (body, None),
                    (None, None) => return true,
                };
                let committed = queue.try_enqueue_pair(token, first, second);
                if !committed {
                    slot.set_close_after();
                }
                committed
            }
            writer::Buffer::Direct(flight) => {
                let Some(written) = slot.accept_write(flight.get().header().len(), written) else {
                    return false;
                };
                let body_len = body.as_ref().map_or(0, |body| body.as_ref().len());
                let Some(total) = written.get().checked_add(body_len) else {
                    slot.set_close_after();
                    return false;
                };
                let Some(total) = num::NonZeroUsize::new(total) else {
                    return true;
                };
                let split = body.is_some();
                let mut lease = flight.commit();
                lease.begin(body);
                slot.state.send.begin(written.get(), total, lease);
                if split {
                    slot.resume_send(flights, driver);
                } else {
                    slot.hand_plain(written, flights, driver);
                }
                true
            }
        }
    }
}

impl<const ID: u8, W: wire::Wire, C> ops::Deref for Write<'_, '_, ID, W, C> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer.as_slice()
    }
}

impl<const ID: u8, W: wire::Wire, C> ops::DerefMut for Write<'_, '_, ID, W, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut_slice()
    }
}
