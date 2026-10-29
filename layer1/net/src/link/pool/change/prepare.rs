use dope_core::driver::route::Identity;

use crate::wire::{self, domain};

/// Initializes one exact wire runtime from its binding kind.
pub trait Prepare<'w, 'd, ID: Identity + 'w, W: wire::Spec + 'w>: Sized
where
    'd: 'w,
{
    fn try_runtime(self, limits: domain::Limits) -> crate::Result<W::Runtime<'w, 'd, ID>>;
}

impl<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w> Prepare<'w, 'd, ID, W>
    for domain::Endpoint<'w, W, ID>
where
    W::Bootstrap: domain::Bootstrap<W>,
{
    fn try_runtime(self, limits: domain::Limits) -> crate::Result<W::Runtime<'w, 'd, ID>> {
        self.try_init(limits)
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w> Prepare<'w, 'd, ID, W>
    for domain::Local<'w, W, ID>
where
    W::Bootstrap: domain::Init<W>,
{
    fn try_runtime(self, limits: domain::Limits) -> crate::Result<W::Runtime<'w, 'd, ID>> {
        Ok(self.init(limits))
    }
}
