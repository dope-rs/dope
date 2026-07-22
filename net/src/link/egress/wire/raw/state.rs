use std::pin::Pin;
use std::rc::Rc;

use o3::buffer::BlockPool;
use o3::cell::RawCell;

use super::lease::WireLease;

pub(crate) struct WireState {
    wire: RawCell<Option<WireLease>>,
    pool: Pin<Rc<BlockPool>>,
}

impl WireState {
    pub(crate) fn new(pool: Pin<Rc<BlockPool>>) -> Self {
        Self {
            wire: RawCell::new(None),
            pool,
        }
    }

    pub(crate) fn prepare(&mut self) -> &mut Option<WireLease> {
        let wire = self.wire.get_mut();
        if wire.is_none() {
            *wire = WireLease::acquire(self.pool.clone());
        }
        wire
    }

    pub(crate) fn consume(&self, amount: usize) {
        unsafe {
            self.wire.with_mut(|wire| {
                if let Some(buffer) = wire.as_mut() {
                    buffer.consume(amount);
                    if buffer.is_empty() {
                        wire.take();
                    }
                }
            })
        };
    }

    pub(crate) fn clear(&mut self) {
        self.wire.get_mut().take();
    }
}
