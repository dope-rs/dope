use std::io;
use std::os::fd::BorrowedFd;

use crate::backend::Backend;
use crate::backend::ops::control::ControlBackend;

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
    fn set(
        &mut self,
        fixed_idx: u32,
        level: u32,
        optname: u32,
        value: i32,
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

    fn set(
        &mut self,
        fixed_idx: u32,
        level: u32,
        optname: u32,
        value: i32,
    ) -> Result<(), PushError> {
        <Backend as ControlBackend>::set(self.backend(), fixed_idx, level, optname, value)
    }
}
