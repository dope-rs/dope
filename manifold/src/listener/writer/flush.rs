use std::pin;

use dope_core::driver::{retained, schedule};
use dope_net::link::pool;

use crate::listener::{
    self, handler,
    writer::{
        flow::{self, SlotFlow as _},
        phase::Phase as _,
    },
};

pub(in crate::listener) trait Flush<'d, const ID: u8> {
    fn flush_after_recv(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        refresh_idle: bool,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, E> Flush<'d, ID> for listener::Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    fn flush_after_recv(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        refresh_idle: bool,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        {
            let this = self.as_mut().project();
            if refresh_idle {
                this.schedule.inbound.arm(key, driver.turn_now());
            }
            if let Some(pool::EgressMut {
                flights,
                connection: slot,
                queue,
            }) = this.owner.pool_mut().egress_mut(key)
            {
                slot.sending().flush_pending(flights, driver);
                let deferred = queue.total_bytes() != 0;
                if !slot.send_status().inflight()
                    && matches!(slot.flow(deferred), flow::Flow::Stalled)
                {
                    slot.resume_send(flights, driver);
                }
            }
        }
        self.maybe_close_slot(key, turn, driver);
    }
}
