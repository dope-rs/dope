use dope_core::{
    driver::route::{self, kind},
    io::event::tuning,
};

use crate::{link::pool, wire};

pub struct Tuning<
    'a,
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B,
    const IOV: usize,
> {
    pool: &'a mut pool::Connections<'d, ID, T, W, S, M, B, IOV>,
}

/// Result of installing and tuning one accepted socket.
pub enum Outcome<'d, const ID: u8, R> {
    Ready(pool::Key<'d, ID>),
    Pending,
    Failed(pool::Key<'d, ID>),
    Unavailable,
    Rejected(R),
}

/// Result of consuming one accepted-socket tuning completion.
pub enum Completion<'d, const ID: u8> {
    Ready(pool::Key<'d, ID>),
    Failed(pool::Key<'d, ID>),
    Stale,
}

impl<'a, 'd, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Tuning<'a, 'd, ID, T, W, S, M, B, IOV>
{
    pub(in crate::link::pool) fn new(
        pool: &'a mut pool::Connections<'d, ID, T, W, S, M, B, IOV>,
    ) -> Self {
        Self { pool }
    }

    pub fn complete(self, completion: tuning::Completion) -> Completion<'d, ID> {
        let Some((key, slot)) = self.pool.by_target_mut(completion.token()) else {
            return Completion::Stale;
        };
        if completion.token() != route::Token::from(key).with_kind(kind::TUNING) {
            return Completion::Stale;
        }
        let applied = slot.engine.establish.complete_tuning(completion);
        if !slot.engine.lifecycle.is_closing() && applied {
            return Completion::Ready(key);
        }
        Completion::Failed(key)
    }
}
