use o3::buffer::Shared;

use super::WirePool;
use super::arena::Arena;
use super::config::Config;

/// Long-lived backing storage for an egress arena.
pub struct Storage {
    pub(super) wire: WirePool,
    pub(super) config: Config,
}

impl Storage {
    pub fn with_capacity(capacity: u32) -> Self {
        Self::with_limits(capacity, super::EGRESS_CAP_BYTES)
    }

    pub fn with_limits(capacity: u32, bytes: u32) -> Self {
        Self::with_config(Config::shared(capacity, bytes))
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            wire: WirePool::new(config.wire_bytes() / WirePool::CAPACITY as u32),
            config,
        }
    }

    pub const fn config(&self) -> Config {
        self.config
    }

    pub fn arena<B, const IOV: usize>(&self, lanes: usize) -> Arena<'_, B, IOV> {
        Arena::with_config(self, self.config, lanes)
    }

    pub fn shared_arena(&self, lanes: usize) -> Arena<'_, Shared> {
        self.arena(lanes)
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::with_config(Config::default())
    }
}
