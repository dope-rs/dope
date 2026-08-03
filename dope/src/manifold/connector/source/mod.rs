pub mod explicit;
pub mod health;

use std::time::Instant;

use dope_core::driver::token::SlotIndex;
use dope_net::Transport;
use o3::collections::{SlabGeneration, SlabKey, SlabKeyParts};

use crate::io::socket::addr::Addr;

type DialEpoch = SlabGeneration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DialKey(SlabKeyParts);

impl DialKey {
    const fn new(index: u32, generation: DialEpoch) -> Self {
        Self(SlabKeyParts::from_generation(index, generation))
    }

    const fn from_slab<Tag>(key: SlabKey<Tag>) -> Self {
        Self(key.parts())
    }

    pub const fn index(self) -> u32 {
        self.0.index()
    }

    const fn generation(self) -> DialEpoch {
        self.0.generation()
    }

    const fn parts(self) -> SlabKeyParts {
        self.0
    }
}

pub enum Action {
    Connect { key: DialKey },
    Backoff { min_retry_at: Instant },
    Idle,
}

pub trait Dialer<T: Transport> {
    fn resize(&mut self, max_connections: usize);
    fn dial(&mut self, addr: T::Addr, config: T::StreamConfig) -> Option<DialKey> {
        let _ = (addr, config);
        None
    }
    fn poll_connect(&mut self, now: Instant) -> Action;
    fn has_pending(&self) -> bool {
        true
    }
    fn sock_addr(&self, key: DialKey) -> Option<Addr>;
    fn socket_params(&self, key: DialKey) -> Option<(i32, i32, i32)>;
    fn stream_config(&self, key: DialKey) -> Option<T::StreamConfig> {
        let _ = key;
        None
    }
    fn connect_outcome(&mut self, key: DialKey, success: bool, now: Instant);
    fn connect_deferred(&mut self, key: DialKey, now: Instant) {
        let _ = (key, now);
    }
    fn disconnect(&mut self, key: DialKey, now: Instant);
    fn kill(&mut self, key: DialKey);
    fn bind(&mut self, key: DialKey, local: SlotIndex) {
        let _ = (key, local);
    }
    fn revive(&mut self) {}
}
