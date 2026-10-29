use std::num::NonZero;

use dope_core::platform::wake;
use o3::mem::quota;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Arrival {
    More,
    Last,
}

/// Counts arrivals on one runtime thread and optionally releases an owned peer.
pub struct Barrier {
    remaining: quota::Ledger,
    notify: Option<wake::Notify>,
}

impl Barrier {
    pub const fn new(target: NonZero<usize>, notify: Option<wake::Notify>) -> Self {
        Self {
            remaining: quota::Ledger::new(target.get()),
            notify,
        }
    }

    /// Elects the final arrival once; excess arrivals cannot release the peer again.
    pub fn arrive(&self) -> Result<Arrival, wake::NotifyError> {
        if matches!(self.remaining.take(), quota::Take::Exhausted)
            || self.remaining.remaining() != 0
        {
            return Ok(Arrival::More);
        }
        if let Some(notify) = &self.notify {
            notify.notify()?;
        }
        Ok(Arrival::Last)
    }
}
