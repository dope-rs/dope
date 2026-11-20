use std::mem::MaybeUninit;

use dope_core::driver::{
    self,
    route::Identity,
    schedule::{app, clock, ready},
};
use dope_net::{
    link::{
        egress::{self, data, reserve},
        slot::transmit::outcome::Progress,
    },
    wire,
};
use o3::{
    buffer::resident::bound,
    mem::{Lease, lease::Open, slot},
};

use super::output::{Owner, Reservation};
use crate::network::listener::{
    conn,
    writer::{
        deadline::Slot,
        flow::Flow,
        memory::{self, Claim},
    },
};

pub(super) enum Deferred {
    Absent,
    Ordered,
    Capacity,
}

pub(super) struct Granted<S, F> {
    slot: S,
    flight: F,
}

impl<'c, 'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B: data::raw::Payload<'d>>
    Slot<'c, 'p, 's, 'w, 'd, ID, W, C, B>
{
    pub(super) fn direct<'a>(
        &'a mut self,
        held: &memory::Retention<'p, 's, 'd, ID, B>,
        pending: egress::Pending,
        target: ready::Target<'d>,
    ) -> Result<
        Granted<&'a mut Self, memory::raw::DirectSlot<'p, 's, 'd, ID, B>>,
        (&'a mut Self, Deferred),
    > {
        if !matches!(self.flow(pending), Flow::Clear) {
            let cause = if held.is_fixed() || held.shared().is_some() {
                Deferred::Ordered
            } else {
                Deferred::Absent
            };
            return Err((self, cause));
        }
        let flight = if let Some(home) = self.state.send.home() {
            held.cancel_header(target);
            // SAFETY: this exclusive connection has no native or queued send.
            // Granted keeps the loan inseparable from its original slot; its
            // only consuming transitions install an Output or start that send.
            unsafe { memory::raw::DirectSlot::fixed(home) }
        } else {
            let Some(source) = held.shared() else {
                return Err((self, Deferred::Absent));
            };
            match held.claim_shared(source, target) {
                Claim::Acquired(flight) => flight,
                Claim::Waiting => return Err((self, Deferred::Capacity)),
            }
        };
        Ok(Granted { slot: self, flight })
    }
}

impl<
    'a,
    'c,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B: data::raw::Payload<'d>,
>
    Granted<
        &'a mut Slot<'c, 'p, 's, 'w, 'd, ID, W, C, B>,
        memory::raw::DirectSlot<'p, 's, 'd, ID, B>,
    >
{
    pub(super) fn output<'out, 'e, 'io, Q: reserve::Geometry, G: bound::Bound>(
        self,
        open: &'a conn::Open<'c, 'p, 's, 'e, 'd, ID, B>,
        work: app::Lease<'a, 'd>,
        driver: &'a mut driver::Context<'io, 'd>,
        place: &'out mut MaybeUninit<Lease<Owner<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, Q, G>, Open>>,
    ) -> slot::Occupied<'out, Lease<Owner<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, Q, G>, Open>>
    where
        'd: 'e,
    {
        Owner::insert(
            self.slot,
            open,
            work,
            Reservation::Direct {
                header: self.flight,
                driver,
            },
            place,
        )
    }

    pub(super) fn send(
        self,
        open: &conn::Open<'_, 'p, 's, '_, 'd, ID, B>,
        body: egress::NonEmpty<B>,
        now: clock::Moment<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Progress {
        self.slot
            .state
            .send
            .begin(memory::access::Direct::body(self.flight, body));
        self.slot.ensure_send(now);
        self.slot.advance_send(open.slots, open.permit, driver)
    }
}
