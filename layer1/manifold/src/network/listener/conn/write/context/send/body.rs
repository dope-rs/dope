use compose::Either;
use conn::write;
use dope_core::driver::{retained, route::Identity};
use dope_net::{
    link::egress::{self, data},
    wire,
};
use egress::{
    data::{compose, exec},
    reserve::exact,
};
use memory::access;
use write::body::Admit;

use crate::network::listener::{
    self,
    conn::{
        self,
        write::context::send::{Send, capacity},
    },
    writer::{flow::SlotFlow as _, memory},
};

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
    driver: &'token mut retained::Context<'_, '_, 'd>,
    publish: impl FnOnce(
        exact::Prepared<
            'a,
            'token,
            'e,
            'd,
            Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
            <B::Mode as exec::Mode>::Reserve<{ memory::WRITE_BUF_CAP }>,
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
    let send = match capacity::Capacity::new(scope, driver.driver_ref().clock().now(), lease)
        .try_body(driver.region_token(), load)
    {
        Ok(send) => send,
        Err(unsent) => {
            let (cause, load) = unsent.into_parts();
            return match cause {
                capacity::Unavailable::Deferred(cause) => Admit::Deferred { cause, load },
                capacity::Unavailable::Closed => Admit::Closed(load),
                capacity::Unavailable::Rejected(cause) => Admit::Rejected { cause, load },
            };
        }
    };
    let Send {
        mut slot,
        flights: slots,
        pending: ready,
        buffer,

        lease,
    } = send;
    match buffer {
        memory::Body::Queued { offer } => {
            let wake = slot.io().wake_target();
            let now = driver.driver_ref().clock().now();
            let value = match offer.poll_reserve(driver.region_token(), wake) {
                exact::Outcome::Ready(value) => value,
                exact::Outcome::Pending(load) => {
                    slot.ensure_send(now);
                    return Admit::Deferred {
                        cause: write::Deferred::Capacity,
                        load,
                    };
                }
                exact::Outcome::Rejected(rejected) => {
                    return Admit::Unavailable(rejected);
                }
            };
            publish(value);
        }
        memory::Body::Direct {
            slot: flight,
            value,
        } => {
            slot.state
                .send
                .begin(access::Direct::body(flight, inject(value)));
            slot.ensure_send(driver.driver_ref().clock().now());
            slot.advance_send(slots, ready, driver);
        }
    }
    lease.commit().run(|| ready.wake());
    Admit::Committed
}
