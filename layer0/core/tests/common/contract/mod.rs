pub mod admit;
pub mod credits;
pub mod provided;
pub mod reclaim;
mod scope;
pub use scope::Scope;
pub mod submit;

pub trait MaybeFrom<S, M> {}

impl<T, S> MaybeFrom<S, ()> for T {}
impl<T: From<S>, S> MaybeFrom<S, u8> for T {}

pub fn not_from<T: MaybeFrom<S, M>, S, M>() {}
