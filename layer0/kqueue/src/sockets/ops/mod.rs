use dope_sys::{
    engine::admit,
    io::{
        reactor::ready::{
            self,
            store::{Context, Exec, submit},
        },
        request,
        socket::option::tuning::{self, device},
    },
    table::change,
};

use crate::{Command, Kqueue, command::Retained, reactor::Engine, sockets::Handle};

type Key<C> = <<C as ready::Driver>::Context as Exec>::Key;
type Slot<C> = <<<C as ready::Driver>::Context as Context>::Slots as change::raw::Slots>::Physical;

/// Holds the affine descriptor and host access through native socket settlement.
#[repr(transparent)]
pub struct Operation<H> {
    host: H,
}

impl<H> device::Owner<H> for Operation<H> {
    fn new(host: H) -> Self {
        Self { host }
    }
}

mod raw;
