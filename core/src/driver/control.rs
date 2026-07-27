use std::io;
use std::os::fd::BorrowedFd;

use crate::backend::Backend;
use crate::backend::ops::raw::control::ControlBackend;
use crate::io::fd::Fd;
use crate::io::socket::option::SocketOption;

use super::token::Token;
use super::{DriverContext, OutboundReservation, PushError};

pub trait ContextControl {
    fn prepare_drop(&mut self);
    fn register_shutdown_fd(&mut self, fd: BorrowedFd<'_>) -> io::Result<()>;
    fn reserve_outbound(&mut self, count: u32) -> io::Result<OutboundReservation>;
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

    fn reserve_outbound(&mut self, count: u32) -> io::Result<OutboundReservation> {
        <Backend as ControlBackend>::reserve_outbound(self.backend(), count)
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
        <Backend as ControlBackend>::quiesce(self.backend(), targets)
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
