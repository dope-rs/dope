use std::cell::Cell;

use o3::{alloc::VecExt as _, depot::queue::ring::inline};

use crate::app::creation;

/// Fixed append buffers retained for every queued or submitted block lease.
pub struct Storage<const N: usize, const B: usize> {
    pub(super) pool: inline::Pool<Vec<u8>, N>,
    pub(super) failure: Cell<Option<super::Failure>>,
    pub(super) confirmed: Cell<u64>,
}

impl<const N: usize, const B: usize> Storage<N, B> {
    pub fn try_new() -> Result<Self, creation::Error> {
        crate::AppendShape::validate::<N, B>()
            .map_err(crate::ConfigError::AppendGeometry)
            .map_err(creation::Error::Config)?;
        let mut blocks = [const { Vec::new() }; N];
        for bytes in &mut blocks {
            *bytes = Vec::try_new(B).map_err(creation::Error::Allocation)?;
        }
        Ok(Self {
            pool: inline::Pool::new(blocks),
            failure: Cell::new(None),
            confirmed: Cell::new(0),
        })
    }

    /// Returns the most recent appender's uncleared failure after it is dropped.
    /// A successful open starts a new result; draining does not imply durability.
    pub fn failure(&self) -> Option<super::Failure> {
        self.failure.get()
    }

    /// Bytes confirmed by native writes, preserved across rotation and after finish.
    pub fn confirmed(&self) -> u64 {
        self.confirmed.get()
    }
}
