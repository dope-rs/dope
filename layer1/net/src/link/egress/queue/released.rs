use o3::base::cell::region;

use crate::link::egress::{data, load::credit, metadata, queue::flight, reserve::demand};

#[must_use]
pub(in crate::link) struct Released<
    'flight,
    'queue,
    'p,
    'd,
    const IOV: usize,
    B,
    D: demand::Mode = demand::Fallible,
> {
    entries: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
    flight: flight::Released<'flight, 'p, 'd, IOV>,
}

impl<'flight, 'queue, 'p, 'd, const IOV: usize, B: data::raw::Payload<'d>, D: demand::Mode>
    Released<'flight, 'queue, 'p, 'd, IOV, B, D>
{
    pub(super) const fn new(
        entries: metadata::Queue<'queue, 'p, 'd, B, credit::Ready<'p, 'd>, D>,
        flight: flight::Released<'flight, 'p, 'd, IOV>,
    ) -> Self {
        Self { entries, flight }
    }

    /// Acknowledges the released prefix and transfers each completed payload.
    pub(in crate::link) fn consume(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        mut release: impl FnMut(B),
    ) {
        self.flight.consume(self.entries, token, |consumed| {
            consumed.complete(&mut release)
        });
    }
}
