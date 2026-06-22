use crate::backend;

pub struct WakerSet {
    entries: Vec<backend::park::WakeRef>,
}

impl WakerSet {
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn register(&mut self, w: backend::park::WakeRef) {
        if self.entries.contains(&w) {
            return;
        }
        self.entries.push(w);
    }

    pub fn drain_wake(&mut self) {
        for w in self.entries.drain(..) {
            w.wake();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for WakerSet {
    fn default() -> Self {
        Self::new()
    }
}
