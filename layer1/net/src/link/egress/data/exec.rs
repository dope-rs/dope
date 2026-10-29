use std::{marker, num};

use o3::buffer::resident::bound;

use crate::link::egress::{ResidentSize, data, reserve::demand};

/// Type-level execution contract for one egress payload.
pub trait Mode {
    const DIRECT_MINIMUM: Option<num::NonZero<u32>>;
    const EXECUTION: data::Execution;
    type Reserve<const PREFIX: usize>: demand::Mode;
}

/// Payload whose capacity failure remains recoverable.
pub struct Fallible;

impl Mode for Fallible {
    const DIRECT_MINIMUM: Option<num::NonZero<u32>> = None;
    const EXECUTION: data::Execution = data::Execution::Fallible;
    type Reserve<const PREFIX: usize> = demand::Fallible;
}

/// Payload whose outer bound proves startup capacity can be guaranteed.
pub struct Guaranteed<G>(marker::PhantomData<fn() -> G>);

impl<G: bound::Bound> Mode for Guaranteed<G> {
    const DIRECT_MINIMUM: Option<num::NonZero<u32>> = Some(num::NonZero::<u32>::MIN);
    const EXECUTION: data::Execution = match G::LIMIT {
        bound::Limit::AtMost(body) => data::Execution::Bounded(ResidentSize::new(body)),
        bound::Limit::Unbounded => data::Execution::Unbounded,
    };
    type Reserve<const PREFIX: usize> = demand::Guaranteed<G, PREFIX>;
}

const _: () = {
    assert!(std::mem::size_of::<Fallible>() == 0);
    assert!(std::mem::size_of::<Guaranteed<bound::AtMost<0>>>() == 0);
};
