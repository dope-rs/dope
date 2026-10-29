use crate::{
    engine::ring::Context,
    io::Close,
    ring::{map::Unmap, ready::Ready},
};

/// Borrows the native reactor and its stable option storage from one owner.
pub trait Backend<C: Context>: Close<State: Unmap> + Sized {
    type Ring: Ready<Token = C::Token, Layout = C::Layout>;

    fn new(ring: Self::Ring, tuning: C::Tuning) -> Self;
    fn ring(&mut self) -> &mut Self::Ring;
    fn tuning(&mut self) -> &mut C::Tuning;
    fn split(&mut self) -> (&mut Self::Ring, &mut C::Tuning);
}
