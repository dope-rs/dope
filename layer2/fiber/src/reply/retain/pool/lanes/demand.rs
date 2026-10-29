use o3::mem::credit::fair;

use crate::reply::retain::grant::payload;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Demand {
    Data,
    Values(payload::Values),
    Bytes(payload::Bytes),
}

impl Demand {
    pub(super) const fn axis(self) -> fair::Axis<3> {
        match self {
            Self::Data => fair::Axis::at::<0>(),
            Self::Values(_) => fair::Axis::at::<1>(),
            Self::Bytes(_) => fair::Axis::at::<2>(),
        }
    }
}
