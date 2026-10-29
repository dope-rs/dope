mod feature;

pub use feature::Feature;
use o3::base::num::bounded;

type Rejected<S> = (
    S,
    Rejection<<S as Prepare>::Cancel, <S as Prepare>::Files, <S as Prepare>::Range>,
);

/// Admits features and descriptor ranges while retaining every rejected owner.
pub trait Prepare: Sized {
    type Cancel;
    type Files;
    type Range;

    fn prepare<const FILES: u32, const RANGE: u32>(
        self,
        files: bounded::U32<1, FILES>,
        range: Option<bounded::U32<1, RANGE>>,
    ) -> Result<Self, Rejected<Self>>;
}

/// Exact feature or registration failure during ring admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Rejection<C, F, R> {
    Feature(Feature),
    Cancel(C),
    Files(F),
    Range(R),
}
