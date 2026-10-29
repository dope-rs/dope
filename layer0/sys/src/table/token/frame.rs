use crate::table::token::{Context, Tuning};

pub enum Control<T> {
    Shutdown,
    Tuning(T),
    ClosePrep,
    Unknown,
}

/// A validated framework identity retaining its original diagnostic value.
pub trait Decode: Copy {
    type Context: Context;
    type Tuning: Tuning<Slot = <Self::Context as Context>::Slot>;

    fn from_raw(raw: u64) -> Result<Self, u64>;
    fn decode(self) -> Control<Self::Tuning>;
    fn control(self) -> <Self::Context as Context>::Control;
}
