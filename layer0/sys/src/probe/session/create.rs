use crate::{memory, probe::session::Empty};

/// Capability setup retains the complete native owner on every failed stage.
pub trait Create<P>: Empty {
    type Cleanup: memory::Cleanup<P>;

    fn try_new(ring: Self::Ring) -> Result<Self, Result<P, Self::Cleanup>>;
    fn socket(self) -> Result<Self, Result<P, Self::Cleanup>>;
    fn finish(self) -> Result<Self::Ring, Result<P, Self::Cleanup>>;
}
