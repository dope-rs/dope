use dope_core::{
    driver::route::Identity,
    io::{socket::msg, transfer, transfer::progress},
};
use dope_net::{link::egress::data, wire::send};
use o3::{depot::array, mem::slot};

use crate::network::listener;

use super::{Direct, Fault, Prepared, Source, State, Step};

impl<'d, ID: Identity, B: data::raw::Payload<'d>> Direct<'_, '_, 'd, ID, B> {
    pub(in crate::network::listener::writer) fn prepare(&mut self) -> Result<Prepared<'_>, Fault> {
        match self.source {
            Source::Header => Ok(self.prepare_header()),
            Source::Body => self.prepare_body(),
        }
    }

    fn prepare_header(&mut self) -> Prepared<'_> {
        let State::Active(active) = &self.progress.0 else {
            return Prepared::Idle;
        };
        let this = self.lease.get_mut().project();
        // SAFETY: this phase admits only the initialized header. Confirmed
        // progress retains its bounded suffix after every partial completion.
        let bytes = unsafe {
            this.header.into_ref().as_slice()
                .get_unchecked(active.sent.len().into_raw()..self.head.transfer().into_raw())
                .assume_init_ref()
        };
        // SAFETY: this exact lease pins the header through native retirement.
        let plain = unsafe { send::raw::Plain::retain(bytes) };
        Prepared::Plain(plain, Step::new(&mut self.progress))
    }
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> Direct<'_, '_, 'd, ID, B> {
    fn prepare_body(&mut self) -> Result<Prepared<'_>, Fault> {
        let State::Active(active) = &self.progress.0 else {
            return Ok(Prepared::Idle);
        };
        let end = self.head.transfer();
        let consumed = active.sent.len();
        let (start, offset) = if consumed < end {
            (consumed, transfer::Len::ZERO)
        } else {
            (end, consumed.abs_diff(end))
        };
        let this = self.lease.get_mut().project();
        // SAFETY: confirmed progress derives this range from the initialized header.
        let header = unsafe {
            this.header.into_ref().as_slice()
                .get_unchecked(start.into_raw()..end.into_raw())
                .assume_init_ref()
        };
        let mut sent = progress::State::new(active.sent.left());
        let mut count = array::Len::<{ listener::IOV_CAP }>::ZERO;
        if !header.is_empty() {
            let step = sent.reserve(transfer::Len::prefix(header.len(), sent.left()));
            // SAFETY: the reservation bounds this pinned header prefix.
            let bytes = unsafe { header.get_unchecked(..step.amount().into_raw()) };
            let iovec = unsafe { msg::raw::Iovec::retain(bytes) };
            this.iovecs[0].write(iovec);
            count = array::Len::ONE;
            step.commit();
        }
        if !sent.is_empty() {
            // SAFETY: only body admission constructs this phase, after writing
            // the source into its final Flight. It remains initialized here.
            let source = unsafe { this.source.assume_init_ref() };
            // SAFETY: accepted total and confirmed progress bound this offset;
            // speculative scatter never advances the confirmed checkpoint.
            let mut chunks = unsafe { source.resume(this.position, data::Offset::from_len(offset)) };
            while let Some(vacant) = count.vacancy() {
                let left = sent.left();
                if left.is_zero() {
                    break;
                }
                let Some(chunk) = sent.scatter(&mut chunks) else {
                    let extent = transfer::Len::prefix(source.len(), transfer::Len::MAX);
                    return Err(Fault::new(extent.abs_diff(left), extent, left));
                };
                // SAFETY: this lease retains the source through native retirement.
                let iovec = unsafe { msg::raw::Iovec::retain(chunk.bytes()) };
                vacant.get_mut(this.iovecs).write(iovec);
                count = vacant.commit();
            }
        }
        // SAFETY: Flight pins the message, descriptors and their sources.
        let raw = unsafe {
            msg::raw::Vectored::retain((&mut *this.message, &*this.iovecs, count, sent.len()))
        };
        Ok(Prepared::Many(send::Vectored::from_message(raw), Step::new(&mut self.progress)))
    }
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> Direct<'_, '_, 'd, ID, B> {
    pub(in crate::network::listener::writer) fn retire<'out>(
        mut self,
        place: slot::Vacant<'out, B>,
    ) -> Option<slot::Occupied<'out, B>> {
        if matches!(self.source, Source::Header) {
            return None;
        }
        self.source = Source::Header;
        // SAFETY: the quiescent body phase owns this initialized source once.
        // The destination now owns it when the original flight returns to its lane.
        Some(place.insert(unsafe { self.lease.get_mut().project().source.assume_init_read() }))
    }
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> Drop for Direct<'_, '_, 'd, ID, B> {
    fn drop(&mut self) {
        if matches!(self.source, Source::Body) {
            // SAFETY: the private body phase owns this initialized source;
            // normal teardown retires it before the backing lease is returned.
            unsafe { self.lease.get_mut().project().source.assume_init_drop() };
        }
    }
}
