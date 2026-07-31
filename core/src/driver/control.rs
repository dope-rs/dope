use std::io;
use std::os::fd::BorrowedFd;

use crate::backend::Backend;
use crate::backend::ops::raw::control::{ControlBackend, RawQuiesce};
use crate::io::fd::Fd;
use crate::io::socket::option::SocketOption;

use super::token::Token;
use super::{DriverContext, OutboundReservation, PushError};

#[doc(hidden)]
#[must_use]
pub struct Quiesce<'a> {
    backend: &'a mut Backend,
    state: RawQuiesce,
    targets: bool,
}

#[doc(hidden)]
#[must_use]
pub struct QuiesceOutcome {
    targets: bool,
    poison: bool,
}

impl QuiesceOutcome {
    pub fn has_targets(&self) -> bool {
        self.targets
    }

    pub fn needs_poison(&self) -> bool {
        self.poison
    }
}

impl<'a> Quiesce<'a> {
    fn new(backend: &'a mut Backend) -> Self {
        Self {
            backend,
            state: <Backend as ControlBackend>::begin_quiesce(),
            targets: false,
        }
    }

    /// Synchronously revokes backend access to `target`.
    ///
    /// Once this returns, the owner may release memory retained by the
    /// submission. [`Self::finish`] performs any remaining backend-owned
    /// result reclamation for the complete batch.
    pub fn cancel(&mut self, target: Token) {
        <Backend as ControlBackend>::quiesce_target(self.backend, &mut self.state, target);
        self.targets = true;
    }

    pub fn finish(self) -> QuiesceOutcome {
        QuiesceOutcome {
            targets: self.targets,
            poison: <Backend as ControlBackend>::finish_quiesce(self.backend, self.state),
        }
    }
}

pub trait ContextControl {
    fn prepare_drop(&mut self);
    fn register_shutdown_fd(&mut self, fd: BorrowedFd<'_>) -> io::Result<()>;
    fn reserve_route(&mut self, id: u8) -> bool;
    fn release_route(&mut self, id: u8);
    fn poison_route(&mut self, id: u8);
    fn quiesce(&mut self, targets: &[Token]) -> bool;
    /// Queues an option without waiting for its kernel completion.
    fn submit_option(
        &mut self,
        fd: &Fd<'_>,
        option: impl Into<SocketOption>,
    ) -> Result<(), PushError>;
}

impl ContextControl for DriverContext<'_, '_> {
    fn prepare_drop(&mut self) {
        <Backend as ControlBackend>::prepare_drop(self.backend());
    }

    fn register_shutdown_fd(&mut self, fd: BorrowedFd<'_>) -> io::Result<()> {
        <Backend as ControlBackend>::register_shutdown_fd(self.backend(), fd)
    }

    fn reserve_route(&mut self, id: u8) -> bool {
        <Backend as ControlBackend>::reserve_route(self.backend(), id)
    }

    fn release_route(&mut self, id: u8) {
        <Backend as ControlBackend>::release_route(self.backend(), id);
    }

    fn poison_route(&mut self, id: u8) {
        <Backend as ControlBackend>::poison_route(self.backend(), id);
    }

    fn quiesce(&mut self, targets: &[Token]) -> bool {
        let mut quiesce = Quiesce::new(self.backend());
        for target in targets {
            quiesce.cancel(*target);
        }
        quiesce.finish().needs_poison()
    }

    fn submit_option(
        &mut self,
        fd: &Fd<'_>,
        option: impl Into<SocketOption>,
    ) -> Result<(), PushError> {
        let option = option.into();
        <Backend as ControlBackend>::submit_option(
            self.backend(),
            fd.slot(),
            option.level(),
            option.name(),
            option.value(),
        )
    }
}

impl<'d> DriverContext<'_, 'd> {
    #[doc(hidden)]
    pub fn quiesce_batch(&mut self) -> Quiesce<'_> {
        Quiesce::new(self.backend())
    }

    pub fn reserve_outbound(&mut self, count: u32) -> io::Result<OutboundReservation<'d>> {
        let base = <Backend as ControlBackend>::reserve_outbound(self.backend(), count)?;
        Ok(OutboundReservation::new(base, count))
    }

    #[doc(hidden)]
    pub fn retire_outbound(&mut self, reservation: OutboundReservation<'d>) {
        let (base, count) = reservation.into_range();
        <Backend as ControlBackend>::retire_fixed(self.backend(), base, count);
    }

    #[doc(hidden)]
    pub fn retire_fixed_fd(&mut self, fd: &mut Fd<'d>) {
        let Some(slot) = fd.retire_slot(self.driver_ref()) else {
            return;
        };
        self.backend().close_fd(slot);
        <Backend as ControlBackend>::retire_fixed(self.backend(), slot.raw(), 1);
    }
}
