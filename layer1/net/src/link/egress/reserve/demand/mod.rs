use std::{marker, mem, num, num::NonZero};

pub use body::Body;
use dope_core::{driver::route::table::conn, io::transfer};
use o3::buffer::resident::bound;
pub use plan::Plan;
pub use prefix::Prefix;

use crate::link::egress::{self, metadata, reserve};

mod body;
mod plan;
mod prefix;

/// Reserve policy carried by one retained egress storage.
pub trait Mode: Sized {
    /// Classifies the exact retained storage as queued or direct-only.
    fn provision(config: egress::Config, routes: conn::Capacity) -> Provision<Self>;
}

/// Exact final-path provision carried into egress storage construction.
pub enum Provision<M: Mode> {
    Queued {
        setup: Setup<M>,
        headers: NonZero<u32>,
    },
    Direct(Setup<M>),
}

/// Storage whose queue admission remains recoverable.
pub struct Fallible;

/// Storage used by an execution contract with a guaranteed final path.
pub struct Guaranteed<G, const PREFIX: usize> {
    capacity: Option<Capacity<PREFIX>>,
    bound: marker::PhantomData<fn() -> G>,
}

impl<G, const PREFIX: usize> Guaranteed<G, PREFIX> {
    pub(in crate::link::egress) const fn bounds(&self) -> Option<reserve::Bounds> {
        match self.capacity {
            Some(capacity) => Some(reserve::Bounds::proven(capacity)),
            None => None,
        }
    }
}

/// Exact configuration and route capacity awaiting storage construction.
#[must_use]
pub struct Setup<M: Mode> {
    config: egress::Config,
    routes: conn::Capacity,
    mode: M,
}

impl<M: Mode> Setup<M> {
    pub(in crate::link) const fn capacity(&self) -> conn::Capacity {
        self.routes
    }

    pub(in crate::link::egress) fn into_parts(self) -> (egress::Config, metadata::LaneCount, M) {
        (self.config, self.routes.into(), self.mode)
    }
}

impl Fallible {
    /// Prepares recoverable queue admission without a provision branch.
    pub fn prepare(config: egress::Config, routes: conn::Capacity) -> Setup<Self> {
        Setup {
            config,
            routes,
            mode: Self,
        }
    }
}

impl Mode for Fallible {
    fn provision(config: egress::Config, routes: conn::Capacity) -> Provision<Self> {
        Provision::Direct(Self::prepare(config, routes))
    }
}

impl<G: bound::Bound, const PREFIX: usize> Mode for Guaranteed<G, PREFIX> {
    fn provision(config: egress::Config, routes: conn::Capacity) -> Provision<Self> {
        const { assert!(PREFIX > 0 && PREFIX <= transfer::MAX_BYTES) }
        let mut setup = Setup {
            config,
            routes,
            mode: Self {
                capacity: None,
                bound: marker::PhantomData,
            },
        };
        let bound::Limit::AtMost(body) = G::LIMIT else {
            return Provision::Direct(setup);
        };
        let Some(total) = PREFIX.checked_add(body) else {
            return Provision::Direct(setup);
        };
        if total > transfer::MAX_BYTES {
            return Provision::Direct(setup);
        }
        let chunk = transfer::NonZero::fixed_size::<PREFIX>();
        let body = body as u32;
        let resident = egress::Resident::new(total as u32);
        let req = egress::Credit::new(egress::Entries::new(2), resident);
        match setup.config.fits_all(setup.routes.into(), req) {
            egress::Fit::Admitted => {}
            egress::Fit::Entries | egress::Fit::Resident => {
                return Provision::Direct(setup);
            }
        }
        let Some(headers) = setup.config.chunks(chunk) else {
            return Provision::Direct(setup);
        };
        setup.mode.capacity = Some(Capacity::from_valid(body));
        Provision::Queued { setup, headers }
    }
}

const _: () = {
    assert!(mem::size_of::<Fallible>() == 0);
    assert!(
        mem::size_of::<Setup<Fallible>>() == mem::size_of::<(egress::Config, conn::Capacity)>()
    );
    assert!(
        mem::align_of::<Setup<Fallible>>() == mem::align_of::<(egress::Config, conn::Capacity)>()
    );
    assert!(mem::size_of::<Capacity<1>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Option<Capacity<1>>>() == mem::size_of::<Capacity<1>>());
    assert!(mem::size_of::<Guaranteed<bound::AtMost<0>, 1>>() == mem::size_of::<u32>());
};

/// Compact queue-capacity proof for one bounded body and copied prefix.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::link::egress) struct Capacity<const PREFIX: usize>(num::NonZero<u32>);

impl<const PREFIX: usize> Capacity<PREFIX> {
    const fn from_valid(body: u32) -> Self {
        // SAFETY: Proof admits body only when PREFIX + body <= MAX_BYTES,
        // whose bound is strictly below u32::MAX.
        Self(unsafe { num::NonZero::<u32>::new_unchecked(body + 1) })
    }

    pub(in crate::link::egress) const fn body(self) -> u32 {
        self.0.get() - 1
    }
}
