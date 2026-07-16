use std::marker::PhantomData;

use dope_core::driver::token::{SlotIndex, Token};

#[repr(transparent)]
pub struct TypedToken<M>(Token, PhantomData<*mut M>);

impl<M> Clone for TypedToken<M> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<M> Copy for TypedToken<M> {}

impl<M> TypedToken<M> {
    /// # Safety
    /// The caller guarantees `t` was issued for manifold `M`.
    pub const unsafe fn new_unchecked(token: Token) -> Self {
        Self(token, PhantomData)
    }
    pub const fn into_inner(self) -> Token {
        self.0
    }
    pub const fn slot(self) -> SlotIndex {
        self.0.slot()
    }
}
