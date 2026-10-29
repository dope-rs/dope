use dope_core::driver::{self, route::Identity};

use crate::{
    link::{egress::reserve::demand, pool::recv::input},
    wire::{self, domain},
};

pub mod accept;
pub mod close;
pub mod drain;
mod finalize;
pub use finalize::Finalize;
pub mod opening;
mod prepare;
mod quiesce;
pub mod raw;
pub use prepare::Prepare;

/// Pairs one admitted rearm allocation with the selected receive-mode storage.
pub fn try_prepare<'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D, R, C, O>(
    retain: domain::Retained,
    bytes: domain::Resident,
    setup: demand::Setup<D>,
    end: R,
    owner: &mut C,
    context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
    build: impl for<'p> FnOnce(raw::Admission<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>, &mut C) -> O,
) -> crate::Result<O>
where
    'd: 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
    R: Prepare<'w, 'd, ID, W>,
{
    raw::Storage::<ID, B, IOV, D>::try_with(setup, owner, &context, |storage, owner| {
        let pool = storage.prepare(retain, bytes, end, &mut context(owner))?;
        Ok(build(pool, owner))
    })?
}
