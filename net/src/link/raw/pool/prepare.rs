use dope_core::driver::OutboundReservation;

use super::Pool;
use crate::Transport;
use crate::wire::Wire;

#[doc(hidden)]
pub struct PreparedPool<'d, const ID: u8, T: Transport, W: Wire, S>(
    pub(super) Pool<'d, ID, T, W, S>,
);

impl<'d, const ID: u8, T: Transport, W: Wire, S> PreparedPool<'d, ID, T, W, S> {
    pub fn bind(mut self, reservation: OutboundReservation<'d>) -> Pool<'d, ID, T, W, S> {
        self.0.reservation = reservation;
        self.0
    }
}
