use std::marker::PhantomData;

use crate::backend;
use crate::manifold::Manifold;

#[repr(transparent)]
pub struct TypedToken<M: Manifold>(backend::token::Token, PhantomData<fn() -> M>);

impl<M: Manifold> Clone for TypedToken<M> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<M: Manifold> Copy for TypedToken<M> {}

impl<M: Manifold> TypedToken<M> {
    /// # Safety
    /// The caller guarantees `t` was issued for manifold `M`.
    pub const unsafe fn from_raw_token(t: backend::token::Token) -> Self {
        Self(t, PhantomData)
    }
    pub const fn token(self) -> backend::token::Token {
        self.0
    }
    pub const fn slot(self) -> backend::token::LocalIdx {
        self.0.slot()
    }
}
