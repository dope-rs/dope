use dope_sys::{
    engine::{admit, ring::Context},
    io::socket::option::tuning::{self, device},
    table::token::{self, Codec, host},
};

use crate::{Engine, Uring, driver::tuning::Table, reactor::sq};

type Key<C> = <<<C as Context>::Token as Codec>::Context as host::Context>::Key;
type Index<C> = <<<C as Context>::Token as Codec>::Context as token::Context>::Slot;

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
