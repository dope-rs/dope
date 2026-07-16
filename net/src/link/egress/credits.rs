use super::config::Config;

pub(super) struct Credits {
    entries: Balance,
    bytes: Balance,
    held: Box<[Held]>,
}

#[derive(Clone, Copy, Default)]
struct Held {
    entries: usize,
    bytes: usize,
}

struct Debit {
    entries: usize,
    bytes: usize,
}

struct Reserve {
    base: usize,
    remainder: usize,
}

impl Reserve {
    fn new(total: usize, lanes: usize) -> Self {
        Self {
            base: total / lanes,
            remainder: total % lanes,
        }
    }

    fn get(&self, lane: usize) -> usize {
        self.base + usize::from(lane < self.remainder)
    }
}

struct Balance {
    available: usize,
    protected: usize,
    reserve: Reserve,
}

impl Balance {
    fn new(total: usize, reserved: usize, lanes: usize) -> Self {
        Self {
            available: total,
            protected: reserved,
            reserve: Reserve::new(reserved, lanes),
        }
    }

    fn debit(&self, lane: usize, held: usize, amount: usize) -> Option<usize> {
        if amount > self.available {
            return None;
        }
        let own = self.reserve.get(lane).saturating_sub(held).min(amount);
        (amount - own <= self.available - self.protected).then_some(own)
    }

    fn acquire(&mut self, amount: usize, own: usize) {
        self.available -= amount;
        self.protected -= own;
    }

    fn release(&mut self, lane: usize, held: usize, amount: usize) {
        let reserve = self.reserve.get(lane);
        let before = reserve.saturating_sub(held);
        let after = reserve.saturating_sub(held - amount);
        self.available += amount;
        self.protected += after - before;
    }
}

impl Credits {
    pub(super) fn with_config(config: Config, lanes: usize) -> Self {
        assert!(lanes > 0, "egress credit lanes must be positive");
        Self {
            entries: Balance::new(config.entries(), config.reserved_entries as usize, lanes),
            bytes: Balance::new(config.bytes(), config.reserved_bytes as usize, lanes),
            held: vec![Held::default(); lanes].into_boxed_slice(),
        }
    }

    pub(super) fn lanes(&self) -> usize {
        self.held.len()
    }

    fn debit(&self, lane: usize, held: Held, entries: usize, bytes: usize) -> Option<Debit> {
        Some(Debit {
            entries: self.entries.debit(lane, held.entries, entries)?,
            bytes: self.bytes.debit(lane, held.bytes, bytes)?,
        })
    }

    pub(super) fn acquire(&mut self, lane: usize, entries: usize, bytes: usize) -> bool {
        let Some(&held) = self.held.get(lane) else {
            return false;
        };
        let Some(debit) = self.debit(lane, held, entries, bytes) else {
            return false;
        };
        self.entries.acquire(entries, debit.entries);
        self.bytes.acquire(bytes, debit.bytes);
        let held = &mut self.held[lane];
        held.entries += entries;
        held.bytes += bytes;
        true
    }

    pub(super) fn release(&mut self, lane: usize, entries: usize, bytes: usize) {
        let held = &mut self.held[lane];
        debug_assert!(held.entries >= entries);
        debug_assert!(held.bytes >= bytes);
        self.entries.release(lane, held.entries, entries);
        self.bytes.release(lane, held.bytes, bytes);
        held.entries -= entries;
        held.bytes -= bytes;
    }
}
