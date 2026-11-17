use std::mem::MaybeUninit;

use dope_core::driver::route::Identity;
use dope_net::link::{
    egress::{self, data::compose, load::Value, reserve},
    slot::transmit::outcome::Progress,
};
use o3::{base::num::bounded, buffer::write::Slice, mem::slot};

use crate::network::listener::{
    self,
    conn::write::{self, error},
    writer::memory,
};

use super::{Queued, Reservation, apply_end};

impl<
    'a,
    'c,
    'p,
    's,
    'w,
    'e,
    'io,
    'd: 'w + 'e,
    ID: Identity,
    W: dope_net::wire::Spec + 'w + 'd,
    C,
    B: dope_net::link::egress::data::raw::Payload<'d>,
    Q: reserve::Geometry,
    G: o3::buffer::resident::bound::Bound,
> super::Output<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, Q, G>
{
    pub(super) fn defer(
        output: slot::Occupied<'_, Self>,
        now: dope_core::driver::schedule::clock::Moment<'d>,
    ) {
        let mut output = output.into_uninit();
        // SAFETY: the private preparation is initialized but unpublished. This
        // consumes its sole Lease without running Output's abandonment policy.
        let Self { lease, written: _ } = unsafe { output.get_mut().assume_init_mut() };
        let (owner, reservation) = unsafe { std::ptr::read(lease) }.into_parts();
        let Reservation::Queued(queued) = reservation else {
            unreachable!("output");
        };
        let Queued { header, vacant, pending: _ } =
            G::into_inner::<Queued<'a, 'p, 's, 'e, 'd, ID, B, Q>>(queued);
        drop(header);
        drop(vacant);
        owner.slot.ensure_send(now);
    }

    pub(super) fn publish(
        output: slot::Occupied<'_, Self>,
        body: impl memory::access::Body<B>,
        end: write::End,
    ) -> Result<(), write::error::Mismatch<B>> {
        use write::error::{Bounded, Mismatch};

        let written = length(output.written);
        let total = match body.wire() {
            Some(wire) => match wire.checked_add(written.transfer()) {
                Ok(total) => Some(total),
                Err(cause) => {
                    return Err(Mismatch::Bounded {
                        cause: Bounded::Wire(egress::WireError::Total(cause)),
                        written: output.written,
                        body: body.into_value(),
                        end,
                    });
                }
            },
            None => written.transfer().nonzero(),
        };
        let mut output = output.into_uninit();
        // SAFETY: validation retained this initialized Output's sole destruction
        // owner. Consuming it leaves the same backing without a destructor.
        // Only the Lease owns resources; the exhaustive pattern accounts for
        // the remaining count without moving the whole Output.
        let Self {
            lease,
            written: _,
        } = unsafe { output.get_mut().assume_init_mut() };
        let (owner, reservation) = unsafe { std::ptr::read(lease) }.into_parts();
        match reservation {
            Reservation::Direct { mut header, driver } => match total {
                Some(total) => {
                    body.retain(&mut header);
                    owner.slot.state.send.begin(memory::access::Direct::new(
                        header,
                        written,
                        total,
                    ));
                    owner.slot.ensure_send(driver.driver_ref().clock().now());
                    let progress = owner
                        .slot
                        .advance_send(owner.open.slots, owner.open.permit, driver);
                    let _permit = owner.work.commit();
                    apply_end(owner.slot, end);
                    if !matches!(progress, Progress::Submitted) {
                        owner.open.permit.wake();
                    }
                }
                None => {
                    drop(header);
                    owner.slot.cancel_send();
                    apply_end(owner.slot, end);
                }
            },
            Reservation::Queued(queued) => {
                let Queued {
                    header,
                    vacant,
                    pending,
                } = G::into_inner::<Queued<'a, 'p, 's, 'e, 'd, ID, B, Q>>(queued);
                let header = header
                    .finish(written.nonempty())
                    .map(egress::Load::new)
                    .map(egress::Load::into_left)
                    .map(Value::Unchecked);
                let body = body.into_value().map(Value::into_right);
                let status = match vacant.try_publish(header, body) {
                    Ok(status) => status,
                    Err(cause) => {
                        let (cause, expected, first, second) = cause.into_parts();
                        let first = part(first);
                        let second = part(second);
                        owner.slot.cancel_idle(pending);
                        return Err(Mismatch::Values {
                            cause,
                            expected,
                            first,
                            second,
                            end,
                        });
                    }
                };
                match status {
                    reserve::Publish::Published => {
                        let _permit = owner.work.commit();
                        apply_end(owner.slot, end);
                        owner.open.permit.wake();
                    }
                    reserve::Publish::Empty => {
                        apply_end(owner.slot, end);
                        owner.slot.cancel_idle(pending);
                    }
                }
            }
        }
        Ok(())
    }
}

pub(super) fn borrow<'a>(
    bytes: &'a mut [MaybeUninit<u8>; memory::WRITE_BUF_CAP],
    written: &'a mut usize,
) -> Slice<'a, &'a mut usize> {
    // SAFETY: the private Output starts at zero and lends its count only to
    // Slice. The lease retains this unchanged initialized prefix through the loan.
    unsafe { Slice::from_raw_parts(bytes, written) }
}

pub(super) fn length(written: usize) -> memory::Len {
    // SAFETY: only the bounded Slice advances Output's private count.
    memory::Len::from(unsafe {
        bounded::raw::u32::<0, { memory::WRITE_BUF_CAP as u32 }>(written as u32)
    })
}

pub(super) fn part<'p, 's, 'd, ID: Identity, B>(
    value: Option<Value<compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>>>,
) -> error::Part<B> {
    let Some(value) = value else {
        return error::Part::Empty;
    };
    match value.split() {
        compose::Either::Left(prefix) => error::Part::Prefix(prefix.charge()),
        compose::Either::Right(body) => error::Part::Body(body),
    }
}
