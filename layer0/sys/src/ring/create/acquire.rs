use std::num;

use o3::base::num::bounded;

use crate::{engine::ring::create::Failure, memory, ring::create::Create};

type Resources<S, L, P, R> =
    Failure<<S as Create>::Setup, <S as Create>::Map, P, <S as Acquire<L, P>>::Resources, R>;

type Acquired<S, L, P, R> = Result<(S, Option<<S as Acquire<L, P>>::Range>), Resources<S, L, P, R>>;

/// Acquires the ring and retains every partially admitted native resource.
pub trait Acquire<L, P: Copy>: Create {
    type Range;
    type Resources: memory::Cleanup<P>;

    fn acquire<const FILES: u32, R>(
        submit: num::NonZero<u32>,
        complete: num::NonZero<u32>,
        defer: bool,
        files: bounded::U32<1, FILES>,
        range: Option<bounded::U32<1, { 1 << 24 }>>,
    ) -> Acquired<Self, L, P, R>;
}
