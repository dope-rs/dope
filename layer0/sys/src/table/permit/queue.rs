use o3::depot;

use crate::table::permit;

/// Generation-exact registration storage selected by the native backend.
pub trait Queue: Sized {
    type Key: permit::raw::Key;
    type Index: depot::raw::Compact;
    type Capability;

    fn try_acquire() -> Result<Self::Capability, permit::Absent>;
    fn try_arm(
        &self,
        permit: &Self::Capability,
        index: Self::Index,
        key: Self::Key,
    ) -> Result<(), Self::Key>;
    fn cancel(
        &self,
        permit: &Self::Capability,
        index: Self::Index,
        key: Self::Key,
    ) -> permit::State<Self::Key>;
    fn take(
        &self,
        permit: &Self::Capability,
        index: Self::Index,
        key: Self::Key,
    ) -> permit::State<Self::Key>;
    fn grant_one(&self, permit: &Self::Capability) -> Option<(Self::Index, Self::Key)>;
}
