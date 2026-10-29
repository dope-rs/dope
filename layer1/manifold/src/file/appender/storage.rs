use std::cell::Cell;

use o3::{alloc::VecExt as _, depot::queue::ring::inline};

use crate::{app::creation, file::appender::block};

/// Fixed append buffers retained for every queued or submitted block lease.
pub struct Storage<const N: usize, const B: usize> {
    pub(super) pool: inline::Pool<block::Block<B>, N>,
    pub(super) failure: Cell<Option<super::Failure>>,
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
            pool: inline::Pool::new(blocks.map(|bytes| block::Block { bytes })),
            failure: Cell::new(None),
        })
    }

    /// Returns the most recent appender's terminal failure after it is dropped.
    /// A successful open starts a new result; draining does not imply durability.
    pub fn failure(&self) -> Option<super::Failure> {
        self.failure.get()
    }
}
