use dope_core::io::transfer;
use o3::base::cell::region;

use crate::{
    link::egress::{self, data, load::credit, metadata, queue::flight, reserve::demand},
    wire::send,
};

pub(in crate::link) enum Preparation<'flight, 'queue, 'p, 'd, const IOV: usize, B, D: demand::Mode>
{
    Empty,
    Ready(flight::Plan<'flight, 'queue, 'p, 'd, IOV, B, D>),
}

pub(in crate::link) enum Reservation<'flight, 'queue, 'p, 'd, const IOV: usize, B, D: demand::Mode>
{
    Ready(Prepared<'flight, 'queue, 'p, 'd, IOV, B, D>),
    Active,
    Unavailable,
}

#[must_use]
pub(in crate::link) struct Prepared<'flight, 'queue, 'p, 'd, const IOV: usize, B, D: demand::Mode> {
    flight: flight::Prepared<'flight, 'p, 'd, IOV>,
    entries: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
}

impl<'flight, 'queue, 'p, 'd, const IOV: usize, B: data::raw::Payload<'d>, D: demand::Mode>
    Prepared<'flight, 'queue, 'p, 'd, IOV, B, D>
{
    pub(super) const fn new(
        flight: flight::Prepared<'flight, 'p, 'd, IOV>,
        entries: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
    ) -> Self {
        Self { flight, entries }
    }

    pub(in crate::link) fn vectored(&mut self) -> send::Vectored<'_, IOV> {
        self.flight.vectored()
    }

    pub(in crate::link) fn retain(self) {
        self.flight.retain();
    }

    pub(in crate::link) fn release(
        self,
        bytes: transfer::Len,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Result<(), egress::Invalid> {
        let Self { flight, entries } = self;
        flight.release(bytes)?.consume(entries, token, &mut drop);
        Ok(())
    }
}
