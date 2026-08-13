use o3::buffer::{self, pool};

use crate::wire;

/// Runtime-owned pools for bounded connection-local byte cursors.
/// Layout is transport policy; O3 owns cursor movement and output transactions.
pub struct RuntimeBuffers {
    scratch: buffer::Pool,
    recv: buffer::Pool,
}

impl RuntimeBuffers {
    /// Calculates this runtime pool's slot count without wrapping.
    #[must_use]
    pub fn slot_count(
        limits: wire::RuntimeLimits,
        connection_local_per_connection: usize,
        long_lived_slots: usize,
        transient_slots: usize,
    ) -> Option<usize> {
        limits
            .max_connections()
            .checked_mul(connection_local_per_connection)
            .and_then(|slots| slots.checked_add(long_lived_slots))
            .and_then(|slots| slots.checked_add(transient_slots))
    }

    pub fn try_scratch_for_runtime(
        limits: wire::RuntimeLimits,
        scratch_per_connection: usize,
        scratch_extra: usize,
        scratch_capacity: usize,
    ) -> Result<Self, pool::LayoutError> {
        let scratch_slots = Self::slot_count(limits, scratch_per_connection, scratch_extra, 0)
            .ok_or(pool::LayoutError::SlotOverflow)?;
        Self::try_fixed(scratch_slots, scratch_capacity, 0, 1)
    }

    pub fn try_for_runtime(
        limits: wire::RuntimeLimits,
        scratch_per_connection: usize,
        scratch_capacity: usize,
        recv_extra_capacity: usize,
    ) -> Result<Self, pool::LayoutError> {
        Self::try_for_runtime_with_scratch_extra(
            limits,
            scratch_per_connection,
            0,
            scratch_capacity,
            recv_extra_capacity,
        )
    }

    pub fn try_for_runtime_with_scratch_extra(
        limits: wire::RuntimeLimits,
        scratch_per_connection: usize,
        scratch_extra: usize,
        scratch_capacity: usize,
        recv_extra_capacity: usize,
    ) -> Result<Self, pool::LayoutError> {
        let scratch_slots = Self::slot_count(limits, scratch_per_connection, scratch_extra, 0)
            .ok_or(pool::LayoutError::SlotOverflow)?;
        let recv_slots = Self::slot_count(limits, 0, limits.max_retained_recv_chunks(), 1)
            .ok_or(pool::LayoutError::SlotOverflow)?;
        let recv_capacity = limits
            .max_recv_len()
            .checked_add(recv_extra_capacity)
            .ok_or(pool::LayoutError::CapacityOverflow)?;
        Self::try_fixed(scratch_slots, scratch_capacity, recv_slots, recv_capacity)
    }

    pub fn try_fixed(
        scratch_slots: usize,
        scratch_capacity: usize,
        recv_slots: usize,
        recv_capacity: usize,
    ) -> Result<Self, pool::LayoutError> {
        use o3::buffer::pool::state;

        Ok(Self {
            scratch: buffer::Pool::<state::Uninitialized>::try_new(
                scratch_slots,
                scratch_capacity,
            )?,
            recv: buffer::Pool::<state::Uninitialized>::try_new(recv_slots, recv_capacity)?,
        })
    }

    #[must_use]
    pub fn scratch_pool(&self) -> buffer::Pool {
        self.scratch.clone()
    }

    #[must_use]
    pub fn recv_pool(&self) -> buffer::Pool {
        self.recv.clone()
    }

    #[must_use]
    pub fn try_acquire_scratch(&self) -> Option<pool::Cursor> {
        self.scratch.try_acquire_buffer()
    }

    #[must_use]
    pub fn try_acquire_recv(&self) -> Option<pool::Cursor> {
        self.recv.try_acquire_buffer()
    }
}
