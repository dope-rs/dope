use super::DriverContext;
use crate::backend::Backend;
use crate::backend::ops::buffers::BufferBackend;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub len: usize,
    pub entries: u16,
}

impl Config {
    pub fn for_accept(accept_slots: u32, buf_len: usize, max_entries: u32) -> Self {
        const FLOOR: u32 = 1024;
        const PER_CONNECTION: u32 = 4;
        const DRAIN_BATCH: u32 = 256;

        let buf_len_ratio = (buf_len / 4096).max(1) as u32;
        let high_water_mark = PER_CONNECTION
            .saturating_mul(DRAIN_BATCH)
            .min(accept_slots.max(FLOOR));
        let target = high_water_mark.min(max_entries) / buf_len_ratio;
        Self {
            len: buf_len,
            entries: target.max(FLOOR).min(u16::MAX as u32) as u16,
        }
    }

    pub fn apply_overrides(&mut self, len: usize, entries: u16) {
        if len != 0 {
            self.len = len;
        }
        if entries != 0 {
            self.entries = entries;
        }
    }
}

pub trait Buffers {
    fn buffer_group(&self) -> u16;
    fn buffer_len(&self) -> usize;
    fn buffer_count(&self) -> usize;
}

impl Buffers for DriverContext<'_, '_> {
    fn buffer_group(&self) -> u16 {
        <Backend as BufferBackend>::buffer_group(self.backend_ref())
    }

    fn buffer_len(&self) -> usize {
        <Backend as BufferBackend>::buffer_len(self.backend_ref())
    }

    fn buffer_count(&self) -> usize {
        <Backend as BufferBackend>::buffer_count(self.backend_ref())
    }
}
