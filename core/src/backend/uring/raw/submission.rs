use io_uring::IoUring;

use crate::backend::uring::sqe::Sqe;
use crate::driver::PushError;
use crate::io::fd::FdSlot;
use libc::SHUT_RDWR;

pub(crate) enum Submission {}

impl Submission {
    pub(crate) fn push_once(ring: &mut IoUring, sqe: &Sqe) -> Result<(), PushError> {
        // SAFETY: Sqe proves that the entry's resources outlive kernel use.
        unsafe { ring.submission().push(sqe.entry()) }.map_err(|_| PushError)
    }

    pub(crate) fn push(ring: &mut IoUring, sqe: &Sqe) -> Result<(), PushError> {
        if Self::push_once(ring, sqe).is_ok() {
            return Ok(());
        }
        ring.submit().map_err(|_| PushError)?;
        Self::push_once(ring, sqe)
    }

    pub(crate) fn try_close(ring: &mut IoUring, slot: FdSlot) -> bool {
        let shut = Sqe::shutdown_linked_at(slot, SHUT_RDWR);
        let close = Sqe::close_at(slot);
        let entries = [shut.entry().clone(), close.entry().clone()];
        // SAFETY: the ring copies a linked pair retaining only a live fixed slot.
        unsafe { ring.submission().push_multiple(&entries) }.is_ok()
    }
}
