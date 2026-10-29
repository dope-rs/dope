mod acquire;
mod build;
mod cause;
mod error;
mod failure;
mod primary;
mod retain;
pub use acquire::Acquire;
pub use build::Build;
pub use cause::Cause;
pub use error::Error;
pub use failure::Failure;
use o3::base::num::bounded;
pub use primary::Primary;
pub use retain::Retain;

use crate::{
    io::{Close, reactor::complete},
    memory,
    memory::buffer::provided,
    probe, ring,
    ring::{map, ready},
    table::token,
};

type Creation<S> = Error<<S as Create>::Setup, <S as Create>::Map, <S as Create>::Cleanup>;

/// Selects the native owner of every partially constructed reactor resource.
pub trait Source: probe::Source + provided::Source {
    type Primary: Copy;
    type Failure<T: token::Codec>
    where
        Self: complete::Observe<T>;
    type Resources<T: token::Codec, L: memory::raw::Canary, P: Copy + From<Self::Primary> + From<memory::Error>>: Retain<
            P,
            Error = <Self as Source>::Failure<T>,
            Ring = Self::Ring<T>,
            Probe = <Self as probe::Source>::Cleanup<T, L, P>,
            Canary = Self::Canary<L>,
            Context = Self::Context<L>,
            Buffers = <Self as provided::Source>::Cleanup<L>,
            Producer = Self::Producer<L>,
            Registration = <Self::Ring<T> as ring::Owner>::Registration,
            Registered = <Self::Ring<T> as ring::Owner>::Registered,
            BufferError = <Self::Ready<T, L> as ready::Ready>::BufferError,
        >
    where
        Self: complete::Observe<T>;
}

/// Creates a single-issuer ring with the requested queue capacities.
pub trait Create: Close + Sized {
    type Setup: Copy;
    type Map: Copy;
    type Unmap;
    type Cleanup: memory::Cleanup<Cause<Self::Setup, Self::Map>, Error = map::Failure<Self::Unmap>>;

    fn new<const MIN: u32, const MAX: u32, const CQ: u32>(
        submit: bounded::U32<MIN, MAX>,
        complete: bounded::U32<1, CQ>,
        defer: bool,
    ) -> Result<Self, Creation<Self>>;
}
