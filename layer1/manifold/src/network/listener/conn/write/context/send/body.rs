use conn::write;
use dope_core::driver::{self, route::Identity};
use dope_net::{
    link::{
        egress::{self, data},
        slot::transmit::outcome::Progress,
    },
    wire,
};
use egress::reserve::{error, exact};
use write::{body::Admit, direct::Deferred};

use crate::network::listener::{conn, writer};

pub(in crate::network::listener::conn::write::context) fn send<
    'a,
    'token,
    'p,
    's,
    'w,
    'e,
    'd,
    ID: Identity,
    W,
    C,
    B,
    S,
>(
    load: egress::NonEmpty<S>,
    scope: &'a mut conn::Ctx<'e, '_, 'p, 's, 'w, 'd, ID, W, C, B>,
    driver: &'token mut driver::Context<'_, 'd>,
    publish: impl FnOnce(
        exact::Prepared<
            'a,
            'token,
            'e,
            'd,
            writer::Payload<'p, 's, 'd, ID, B>,
            writer::Reserve<'d, B>,
            S,
        >,
    ),
    inject: impl FnOnce(egress::NonEmpty<S>) -> egress::NonEmpty<B>,
) -> Admit<S>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    S: data::raw::Payload<'d>,
    B: data::raw::Payload<'d> + 'd,
{
    let Some(lease) = scope.work.lease() else {
        scope.slot.io().wake_target().wake();
        return Admit::Deferred {
            cause: write::Deferred::Turn,
            load,
        };
    };
    let now = driver.driver_ref().clock().now();
    let slot = &mut scope.slot;
    if slot.send_status().is_closed() {
        return Admit::Closed(load);
    }
    let Some(open) = scope.output else {
        return Admit::Closed(load);
    };
    let held = &open.held;
    let queue = &open.queue;
    let ready = open.permit;
    let target = slot.io().wake_target();
    let progress = 'admit: {
        let (slot, direct) = match slot.direct(held, queue.pending(), target) {
            Ok(granted) => {
                queue.cancel_wait(driver.region_token());
                break 'admit granted.send(open, inject(load), now, driver);
            }
            Err(other) => other,
        };
        let offer = match queue.offer(load) {
            Ok(offer) => offer,
            Err(error) => {
                let (load, reason) = error.into_parts();
                let cause = match direct {
                    Deferred::Absent => {
                        held.cancel(driver.region_token(), queue, target);
                        return Admit::Rejected {
                            cause: write::Reject::Geometry(error::Full::Reserve(reason)),
                            load,
                        };
                    }
                    Deferred::Ordered => {
                        held.cancel(driver.region_token(), queue, target);
                        write::Deferred::OrderedDrain
                    }
                    Deferred::Capacity => {
                        queue.cancel_wait(driver.region_token());
                        write::Deferred::Capacity
                    }
                };
                slot.ensure_send(now);
                return Admit::Deferred { cause, load };
            }
        };
        held.cancel_wait(target);
        let value = match offer.poll_reserve(driver.region_token(), target) {
            exact::Outcome::Ready(value) => value,
            exact::Outcome::Pending(load) => {
                slot.ensure_send(now);
                return Admit::Deferred {
                    cause: write::Deferred::Capacity,
                    load,
                };
            }
            exact::Outcome::Rejected(rejected) => return Admit::Unavailable(rejected),
        };
        publish(value);
        Progress::Idle
    };
    lease.commit().run(|| {
        if !matches!(progress, Progress::Submitted) {
            ready.wake();
        }
    });
    Admit::Committed
}
