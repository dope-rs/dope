use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

use o3::buffer::{Bytes, CapacityError, Leased, SharedLease, SharedPool, SpareFillError};

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
    /// Removes `n` bytes from the front of this scratch buffer.
    ///
    /// # Panics
    ///
    /// Panics when `n` exceeds the current length.
    #[track_caller]
    pub fn consume(&mut self, n: usize) {
        assert!(n <= self.len(), "wire buffer consume overflow");
        self.head += n as u32;
        if self.head as usize == self.lease.len() {
            self.head = 0;
            self.lease.truncate(0);
        }
    }
}

pub enum FillError<E> {
    Fill(E),
    Capacity,
}

impl<E: fmt::Debug> fmt::Debug for FillError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

pub struct Buffered {
    scratch: SharedPool,
    recv: RecvPool,
}

impl Buffered {
    pub fn try_for_runtime(
        limits: RuntimeLimits,
        scratch_per_connection: usize,
        scratch_capacity: usize,
        recv_extra_capacity: usize,
    ) -> Result<Self, o3::buffer::PoolLayoutError> {
        let scratch_slots = limits
            .max_connections()
            .checked_mul(scratch_per_connection)
            .ok_or(o3::buffer::PoolLayoutError::SlotOverflow)?;
        let recv_slots = limits
            .max_retained_recv_chunks()
            .checked_add(1)
            .ok_or(o3::buffer::PoolLayoutError::SlotOverflow)?;
        let recv_capacity = limits
            .max_recv_len()
            .checked_add(recv_extra_capacity)
            .ok_or(o3::buffer::PoolLayoutError::CapacityOverflow)?;
        Self::try_fixed(scratch_slots, scratch_capacity, recv_slots, recv_capacity)
    }

    pub fn try_fixed(
        scratch_slots: usize,
        scratch_capacity: usize,
        recv_slots: usize,
        recv_capacity: usize,
    ) -> Result<Self, o3::buffer::PoolLayoutError> {
        Ok(Self {
            scratch: SharedPool::try_new(scratch_slots, scratch_capacity)?,
            recv: RecvPool {
                pool: SharedPool::try_new(recv_slots, recv_capacity)?,
            },
        })
    }

    #[must_use]
    pub fn try_acquire_scratch(&self) -> Option<Buffer<Scratch>> {
        self.scratch.try_acquire().map(Buffer::new)
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
