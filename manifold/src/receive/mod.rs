use std::{io, mem};

use dope_net::{link::pool::input, wire};

mod delivery;
#[doc(hidden)]
pub mod ingress;
mod policy;

pub(crate) use delivery::Delivery;
pub(crate) use policy::Policy;

pub struct Borrowed;
pub struct Retained;

/// Declarative receive-retention bound for one connection pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retention {
    per_connection: usize,
    minimum: usize,
}

impl Retention {
    pub const NONE: Self = Self::new(0, 0);

    pub const fn new(per_connection: usize, minimum: usize) -> Self {
        Self {
            per_connection,
            minimum,
        }
    }

    pub fn capacity(self, connections: usize) -> io::Result<usize> {
        connections
            .checked_mul(self.per_connection)
            .map(|capacity| capacity.max(self.minimum))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "retained receive capacity overflow",
                )
            })
    }
}

const _: () = assert!(mem::size_of::<Borrowed>() == 0 && mem::size_of::<Retained>() == 0);

pub trait Mode<W: wire::Wire>: input::Mode<W> + Policy {}

impl Policy for Borrowed {}
impl Policy for Retained {}

impl<W: wire::Wire> input::Mode<W> for Borrowed {
    type Kind = input::Borrowed;

    const DEFERS: bool = <input::Borrowed as input::Mode<W>>::DEFERS;
}

impl<W: wire::Wire> input::Mode<W> for Retained {
    type Kind = input::Retained;

    const DEFERS: bool = <input::Retained as input::Mode<W>>::DEFERS;
}

impl<W: wire::Wire> Mode<W> for Borrowed {}

impl<W: wire::Wire> Mode<W> for Retained {}
