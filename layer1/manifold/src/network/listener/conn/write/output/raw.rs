use std::{mem::MaybeUninit, ptr};

use dope_core::driver::{self, route::Identity};
use dope_net::link::{
    egress::{self, data::compose, load::Value, reserve},
    slot::transmit::outcome::Progress,
};
use o3::{
    base::num::bounded,
    buffer::write::Slice,
    mem::{Lease, lease::Open, slot},
};

use crate::network::listener::{
    self,
    conn::{
        self,
        write::{self, error},
    },
    writer::memory,
};

use super::{Owner, Queue, Queued, Reservation, apply_end};

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
    #[inline(always)]
    pub(in crate::network::listener::conn::write) fn prepare<'out, P>(
        connection: &'a mut conn::Ctx<'e, 'c, 'p, 's, 'w, 'd, ID, W, C, B>,
        driver: &'a mut driver::Context<'io, 'd>,
        output: &'out mut MaybeUninit<Self>,
        prepare: impl FnOnce(&Queue<'c, 'p, 's, 'e, 'd, ID, B>) -> Option<(G::Finite, P)>,
        plan: impl FnOnce(&Queue<'c, 'p, 's, 'e, 'd, ID, B>, P)
            -> Result<memory::Plan<'c, 'p, 's, 'e, 'd, ID, B, Q>, reserve::error::Lane>,
    ) -> Result<slot::Occupied<'out, Self>, error::Admission> {
        let place = output.as_mut_ptr();
        // SAFETY: only the lease field is lent for initialization in the exclusive output slot.
        let lease = unsafe { &mut *uninit(&raw mut (*place).lease) };
        let admitted = Owner::admit(connection, driver, lease, prepare, plan)?;
        let _ = admitted.into_uninit();
        // SAFETY: admission installed that exact lease and transferred its destructor.
        // Initializing the disjoint count completes the output at its original address.
        unsafe {
            (&raw mut (*place).written).write(0);
            Ok(slot::Vacant::new(output).assume_init())
        }
    }

    pub(super) fn defer(
        output: slot::Occupied<'_, Lease<Owner<'a, 'c, 'p, 's, 'w, 'e, 'io, 'd, ID, W, C, B, Q, G>, Open>>,
        now: dope_core::driver::schedule::clock::Moment<'d>,
    ) {
        let mut output = output.into_uninit();
        // SAFETY: the consumed slot retains the initialized, unpublished lease.
        let lease = unsafe { output.get_mut().assume_init_mut() };
        let (owner, reservation) = lease.parts_mut();
        let Reservation::Queued(queued) = reservation else {
            unreachable!("output");
        };
        // SAFETY: the disarmed output transfers its only refundable work and
        // selected queued reservation; reference and marker fields need no drop.
        let work = unsafe { ptr::read(&owner.work) };
        let Queued { header, vacant, pending: _ } =
            G::into_inner::<Queued<'a, 'p, 's, 'e, 'd, ID, B, Q>>(unsafe { ptr::read(queued) });
        drop(header);
        drop(vacant);
        owner.slot.ensure_send(now);
        drop(work);
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
        // SAFETY: consuming the slot disarms Output's abandonment policy while
        // retaining its initialized fields at their final addresses.
        let Self {
            lease,
            written: _,
        } = unsafe { output.get_mut().assume_init_mut() };
        let (owner, reservation) = lease.parts_mut();
        // SAFETY: this is the sole work owner; every exit commits or refunds it.
        let work = unsafe { ptr::read(&owner.work) };
        match reservation {
            Reservation::Direct { header, driver } => {
                // SAFETY: this selected lease is consumed once, with no whole
                // Output/Reservation move or later abandonment destructor.
                let header = unsafe { ptr::read(header) };
                match total {
                    Some(total) => {
                        body.begin(&mut owner.slot.state.send, header, written, total);
                        owner.slot.ensure_send(driver.driver_ref().clock().now());
                        let progress = owner
                            .slot
                            .advance_send(owner.open.slots, owner.open.permit, driver);
                        let _permit = work.commit();
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
                }
            }
            Reservation::Queued(queued) => {
                // SAFETY: only this selected reservation owns these queue leaves.
                let queued = unsafe { ptr::read(queued) };
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
                        let _permit = work.commit();
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

fn uninit<T>(field: *mut T) -> *mut MaybeUninit<T> {
    field.cast()
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
