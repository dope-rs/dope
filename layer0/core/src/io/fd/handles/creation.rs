use std::marker;

use dope_sys::engine::admit::Cancelled;
use o3::mem::Rejected;

use crate::{
    driver::{
        retained::{flight, flight::cancel::Error},
        route,
        route::kind::Socket,
        schedule::ready,
    },
    io::{fd::lease, socket::pending},
};

/// An outbound descriptor slot whose socket creation is in flight.
///
/// Its invariant driver lifetime cannot be shortened into another scope.
///
pub struct Creation<'d, State = pending::Live> {
    pub(super) lease: lease::Lease<'d, lease::Creating>,
    pub(super) flight: flight::Flight<'d, Socket>,
    pub(super) state: marker::PhantomData<State>,
}

impl<'d> Creation<'d> {
    pub fn try_cancel(
        mut self,
        driver: &mut crate::driver::Context<'_, 'd>,
    ) -> Result<(Creation<'d, pending::Cancelled>, Cancelled), Rejected<Self, Error>> {
        let state = match self.flight.try_cancel(driver) {
            Ok(status) => status,
            Err(cause) => return Err(Rejected::new(self, cause)),
        };
        Ok((
            Creation {
                lease: self.lease,
                flight: self.flight,
                state: marker::PhantomData,
            },
            state,
        ))
    }
}

impl<'d, State> Creation<'d, State> {
    pub(in crate::io) fn target(&self) -> route::Erased<'d, Socket> {
        self.flight.target()
    }

    /// Detaches quiesced kernel authority and returns its reserved descriptor slot.
    pub fn detach(self) {
        let lease = self.lease;
        let flight = self.flight;
        drop(flight);
        drop(lease);
    }

    pub fn ready_handle(&self) -> ready::Handle<'d> {
        self.lease.ready_handle()
    }
}
