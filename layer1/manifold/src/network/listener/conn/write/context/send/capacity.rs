use conn::write;
use dope_core::driver::route::Identity;
use dope_net::{
    link::egress::{self, data},
    wire,
};
use o3::base::cell::region;
use writer::{
    deadline::Slot,
    flow::Flow::Clear,
    memory::{self, Buffer, Claim},
};

use crate::network::listener::{conn, writer};

pub(in crate::network::listener::conn::write::context) enum Unavailable {
    Closed,
    Deferred(write::Deferred),
    Rejected(write::Reject),
}

pub(in crate::network::listener::conn::write::context) fn header<
    'e,
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
    P,
    const CANCEL: bool,
>(
    held: &'a mut memory::Retention<'p, 's, 'd, ID, B>,
    queue: &'a egress::Queue<
        'c,
        'e,
        'd,
        { crate::network::listener::IOV_CAP },
        writer::Payload<'p, 's, 'd, ID, B>,
        writer::Reserve<'d, B>,
    >,
    slot: &mut Slot<'_, 'p, 's, 'w, 'd, ID, W, C, B>,
    token: &mut region::Token<region::Identity<'d>>,
    prepare: impl FnOnce(
        &egress::Queue<
            'c,
            'e,
            'd,
            { crate::network::listener::IOV_CAP },
            writer::Payload<'p, 's, 'd, ID, B>,
            writer::Reserve<'d, B>,
        >,
    ) -> Option<P>,
) -> Result<Buffer<'a, 'p, 's, 'e, 'd, ID, B, P>, Unavailable> {
    if slot.send_status().is_closed() {
        return Err(Unavailable::Closed);
    }
    let target = slot.io().wake_target();
    let flow = slot.flow(queue.pending());
    let (direct, header) = held.sources();
    let plan = header.zip(prepare(queue));
    if !matches!(&flow, Clear) && direct.is_some() && plan.is_none() {
        held.cancel(token, queue, target);
        return Err(Unavailable::Deferred(write::Deferred::OrderedDrain));
    }
    if let (Clear, Some(source)) = (&flow, direct) {
        match held.take_direct(source, target) {
            Claim::Acquired(flight) => {
                queue.cancel_wait(token);
                return Ok(Buffer::Direct(flight));
            }
            Claim::Waiting if plan.is_none() => {
                queue.cancel_wait(token);
                return Err(Unavailable::Deferred(write::Deferred::Capacity));
            }
            Claim::Waiting => {}
        }
    }
    let Some((header, plan)) = plan else {
        held.cancel(token, queue, target);
        return Err(Unavailable::Rejected(write::Reject::Header));
    };
    let header = match held.take_header(header, target) {
        Claim::Acquired(header) => header,
        Claim::Waiting => {
            queue.cancel_wait(token);
            return Err(Unavailable::Deferred(write::Deferred::Capacity));
        }
    };
    if CANCEL {
        queue.cancel_wait(token);
    }
    Ok(Buffer::Queued {
        slot: header,
        queue: queue.reborrow(),
        plan,
    })
}
