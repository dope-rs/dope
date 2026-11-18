use dope_core::{
    driver::{self, route::Identity},
    io::{socket::msg, transfer, transfer::progress},
};
use dope_net::{
    link::{
        egress::data, event, pool::pending,
        slot::transmit::{direct::raw, outcome},
    },
    wire::{self, send},
};
use o3::{depot::array, mem::slot};

use crate::network::listener::{self, writer::deadline::Slot};

use super::{Arm, Direct, Fault, Source, State};

impl<'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B: data::raw::Payload<'d>>
    Slot<'_, 'p, 's, 'w, 'd, ID, W, C, B>
{
    pub(in crate::network::listener) fn advance_send(
        &mut self,
        slots: &crate::Flights<'d, ID>,
        pending: pending::Handle<'_, 'd, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> outcome::Progress {
        let Some(direct) = self.state.send.direct() else {
            return outcome::Progress::Idle;
        };
        let State::Active(active) = &direct.progress.0 else {
            return outcome::Progress::Idle;
        };
        let flight = direct.lease.get_mut().project();
        let end = direct.head.transfer();
        let consumed = active.sent.len();
        let storage = flight.header.into_ref().as_slice();
        let header = |start: transfer::Len| {
            // SAFETY: admission initialized the header through `end`. Confirmed
            // header progress or the body boundary supplies a start within it.
            unsafe { storage.get_unchecked(start.into_raw()..end.into_raw()).assume_init_ref() }
        };
        let handoff = match direct.source {
            Source::Header => {
                // SAFETY: the resident Flight keeps its pinned header through
                // completion or native quiescence, including partial writes.
                unsafe {
                    let plain = send::raw::Plain::retain(header(consumed));
                    raw::submit_plain(self.source, slots, driver, plain)
                }
            }
            Source::Body => {
                let (start, offset) = if consumed < end {
                    (consumed, transfer::Len::ZERO)
                } else {
                    (end, consumed.abs_diff(end))
                };
                let header = header(start);
                let mut sent = progress::State::new(active.sent.left());
                let mut count = array::Len::<{ listener::IOV_CAP }>::ZERO;
                if !header.is_empty() {
                    let step = sent.reserve(transfer::Len::prefix(header.len(), sent.left()));
                    // SAFETY: the reservation bounds this pinned header prefix.
                    let bytes = unsafe { header.get_unchecked(..step.amount().into_raw()) };
                    let iovec = unsafe { msg::raw::Iovec::retain(bytes) };
                    flight.iovecs[0].write(iovec);
                    count = array::Len::ONE;
                    step.commit();
                }
                if !sent.is_empty() {
                    // SAFETY: only body admission initializes this Flight source.
                    let source = unsafe { flight.source.assume_init_ref() };
                    // SAFETY: accepted total and confirmed progress bound the offset.
                    // Speculative scatter does not advance the confirmed checkpoint.
                    let mut chunks = unsafe {
                        source.resume(flight.position, data::Offset::from_len(offset))
                    };
                    while let Some(vacant) = count.vacancy() {
                        let left = sent.left();
                        if left.is_zero() {
                            break;
                        }
                        let Some(chunk) = sent.scatter(&mut chunks) else {
                            let extent = transfer::Len::prefix(source.len(), transfer::Len::MAX);
                            direct.progress.0 =
                                State::Failed(Fault::new(extent.abs_diff(left), extent, left));
                            self.source.io().close();
                            return outcome::Progress::Idle;
                        };
                        // SAFETY: the Flight retains this source through native retirement.
                        let iovec = unsafe { msg::raw::Iovec::retain(chunk.bytes()) };
                        vacant.get_mut(flight.iovecs).write(iovec);
                        count = vacant.commit();
                    }
                }
                // SAFETY: the resident Flight pins the initialized message,
                // descriptors and all their backing through completion or quiescence.
                unsafe {
                    let raw = msg::raw::Vectored::retain((
                        &mut *flight.message, &*flight.iovecs, count, sent.len(),
                    ));
                    raw::submit_many(self.source, send::Vectored::from_message(raw), slots, driver)
                }
            }
        };
        let progress = handoff.progress();
        match progress {
            outcome::Progress::Submitted => direct.progress.record(handoff.consumed(), Arm::Armed),
            outcome::Progress::Idle => direct.progress.record(handoff.consumed(), Arm::Idle),
            outcome::Progress::Rejected(error) => {
                direct.progress.0 = State::Rejected(event::SendFailure::Direct(error));
                self.source.discard();
            }
        }
        pending.wait_send(self.source.send_status().is_waiting());
        progress
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
