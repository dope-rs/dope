use std::fmt;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Range;

use o3::buffer::{
    Bytes, CapacityError, Leased, PoolLayoutError, PrefixLength, Retained, SharedLease, SharedPool,
    SpareFillError, Uninitialized, ValidatedPrefix,
};

use super::RuntimeLimits;

pub enum Scratch {}

pub enum Recv {}

pub struct Buffer<R> {
    lease: SharedLease,
    head: u32,
    role: PhantomData<R>,
}

impl<R> Buffer<R> {
    fn new(lease: SharedLease) -> Self {
        Self {
            lease,
            head: 0,
            role: PhantomData,
        }
    }

    #[must_use]
    pub fn spare_capacity(&self) -> usize {
        self.lease.capacity() - self.len()
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.lease.capacity()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.head as usize == self.lease.len()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.lease.as_slice()[self.head as usize..]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.lease.as_mut_slice()[self.head as usize..]
    }

    pub fn try_extend_from_slice(&mut self, src: &[u8]) -> Result<(), CapacityError> {
        if src.len() > self.lease.capacity() - self.lease.len()
            && src.len() <= self.spare_capacity()
        {
            self.compact();
        }
        self.lease.spare_writer().try_extend_from_slice(src)
    }

    pub fn try_fill<E, F>(&mut self, fill: F) -> Result<(), FillError<E>>
    where
        F: for<'a> FnOnce(&'a mut [MaybeUninit<u8>]) -> Result<&'a mut [u8], E>,
    {
        if self.head != 0 {
            self.compact();
        }
        self.lease
            .spare_writer()
            .try_fill(fill)
            .map_err(|error| match error {
                SpareFillError::Fill(error) => FillError::Fill(error),
                SpareFillError::Capacity => FillError::Capacity,
            })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.lease.len() - self.head as usize
    }

    fn compact(&mut self) {
        if self.head == 0 {
            return;
        }
        let len = self.len();
        self.lease
            .as_mut_slice()
            .copy_within(self.head as usize.., 0);
        self.head = 0;
        self.lease.truncate(len);
    }
}

impl Buffer<Recv> {
    #[must_use]
    pub fn freeze(self) -> Bytes<Leased> {
        let Self { lease, .. } = self;
        let pooled = lease.freeze();
        Bytes::<Leased>::from(pooled)
    }
}

impl Buffer<Scratch> {
    pub fn try_consume(&mut self, n: usize) -> bool {
        if n > self.len() {
            return false;
        }
        self.consume_valid(n);
        true
    }

    pub fn try_consume_prefix(
        &mut self,
        amount: usize,
    ) -> Result<ValidatedPrefix<'_, Self, impl FnOnce(&mut Self, usize)>, CapacityError> {
        ValidatedPrefix::try_new(self, amount, Self::consume_valid)
    }

    pub fn consume_prefix_up_to(&mut self, requested: usize) -> usize {
        let prefix = ValidatedPrefix::up_to(self, requested, Self::consume_valid);
        let amount = prefix.len();
        prefix.commit();
        amount
    }

    fn consume_valid(&mut self, n: usize) {
        debug_assert!(n <= self.len());
        self.head += n as u32;
        if self.head as usize == self.lease.len() {
            self.head = 0;
            self.lease.truncate(0);
        }
    }

    #[must_use]
    pub fn freeze_range(self, range: Range<usize>) -> Option<Bytes<Retained>> {
        if range.start > range.end || range.end > self.len() {
            return None;
        }
        let start = self.head as usize + range.start;
        let end = self.head as usize + range.end;
        Bytes::<Retained>::from(self.lease.freeze()).get(start..end)
    }
}

impl<R> PrefixLength for Buffer<R> {
    fn prefix_len(&self) -> usize {
        self.len()
    }
}

pub enum FillError<E> {
    Fill(E),
    Capacity,
}

impl<E: Debug> Debug for FillError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fill(error) => f.debug_tuple("Fill").field(error).finish(),
            Self::Capacity => f.write_str("Capacity"),
        }
    }
}

#[derive(Clone)]
pub struct RecvPool {
    pool: SharedPool,
}

impl RecvPool {
    #[must_use]
    pub fn try_acquire(&self) -> Option<Buffer<Recv>> {
        self.pool.try_acquire().map(Buffer::new)
    }
}

#[derive(Clone)]
pub struct ScratchPool {
    pool: SharedPool,
}

impl ScratchPool {
    pub fn try_new(slots: usize, capacity: usize) -> Result<Self, PoolLayoutError> {
        Ok(Self {
            pool: SharedPool::<Uninitialized>::try_new(slots, capacity)?,
        })
    }

    #[must_use]
    pub fn try_acquire(&self) -> Option<Buffer<Scratch>> {
        self.pool.try_acquire().map(Buffer::new)
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.pool.capacity()
    }

    #[must_use]
    pub fn available(&self) -> usize {
        self.pool.available()
    }
}

pub struct Buffered {
    scratch: SharedPool,
    recv: RecvPool,
}

impl Buffered {
    pub fn try_scratch_for_runtime(
        limits: RuntimeLimits,
        scratch_per_connection: usize,
        scratch_extra: usize,
        scratch_capacity: usize,
    ) -> Result<Self, PoolLayoutError> {
        let scratch_slots = limits
            .max_connections()
            .checked_mul(scratch_per_connection)
            .and_then(|slots| slots.checked_add(scratch_extra))
            .ok_or(PoolLayoutError::SlotOverflow)?;
        Self::try_fixed(scratch_slots, scratch_capacity, 0, 1)
    }

    pub fn try_for_runtime(
        limits: RuntimeLimits,
        scratch_per_connection: usize,
        scratch_capacity: usize,
        recv_extra_capacity: usize,
    ) -> Result<Self, PoolLayoutError> {
        Self::try_for_runtime_with_scratch_extra(
            limits,
            scratch_per_connection,
            0,
            scratch_capacity,
            recv_extra_capacity,
        )
    }

    pub fn try_for_runtime_with_scratch_extra(
        limits: RuntimeLimits,
        scratch_per_connection: usize,
        scratch_extra: usize,
        scratch_capacity: usize,
        recv_extra_capacity: usize,
    ) -> Result<Self, PoolLayoutError> {
        let scratch_slots = limits
            .max_connections()
            .checked_mul(scratch_per_connection)
            .and_then(|slots| slots.checked_add(scratch_extra))
            .ok_or(PoolLayoutError::SlotOverflow)?;
        let recv_slots = limits
            .max_retained_recv_chunks()
            .checked_add(1)
            .ok_or(PoolLayoutError::SlotOverflow)?;
        let recv_capacity = limits
            .max_recv_len()
            .checked_add(recv_extra_capacity)
            .ok_or(PoolLayoutError::CapacityOverflow)?;
        Self::try_fixed(scratch_slots, scratch_capacity, recv_slots, recv_capacity)
    }

    pub fn try_fixed(
        scratch_slots: usize,
        scratch_capacity: usize,
        recv_slots: usize,
        recv_capacity: usize,
    ) -> Result<Self, PoolLayoutError> {
        Ok(Self {
            scratch: SharedPool::<Uninitialized>::try_new(scratch_slots, scratch_capacity)?,
            recv: RecvPool {
                pool: SharedPool::<Uninitialized>::try_new(recv_slots, recv_capacity)?,
            },
        })
    }

    #[must_use]
    pub fn try_acquire_scratch(&self) -> Option<Buffer<Scratch>> {
        self.scratch.try_acquire().map(Buffer::new)
    }

    #[must_use]
    pub fn scratch_pool(&self) -> ScratchPool {
        ScratchPool {
            pool: self.scratch.clone(),
        }
    }

    #[must_use]
    pub fn recv_pool(&self) -> RecvPool {
        self.recv.clone()
    }

    #[must_use]
    pub fn try_acquire_recv(&self) -> Option<Buffer<Recv>> {
        self.recv.try_acquire()
    }
}
