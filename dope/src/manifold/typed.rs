use std::marker::PhantomData;

use dope_core::driver::token::{SlotIndex, Token};

use super::Manifold;

#[repr(transparent)]
pub struct TypedToken<M>(Token, PhantomData<*mut M>);

impl<M> Clone for TypedToken<M> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<M> Copy for TypedToken<M> {}

impl<M> TypedToken<M> {
    pub const fn try_new<'d>(token: Token) -> Option<Self>
    where
        M: Manifold<'d>,
    {
        if token.route() == M::ID {
            Some(Self(token, PhantomData))
        } else {
            None
        }
    }

    /// # Safety
    /// The caller guarantees `t` was issued for manifold `M`.
    pub const unsafe fn new_unchecked(token: Token) -> Self {
        Self(token, PhantomData)
    }

    /// Retags a token while statically proving that both manifolds own the
    /// same runtime route.
    ///
    /// This is the zero-cost forwarding operation for transparent manifold
    /// wrappers. A differing pair of route IDs fails during monomorphization
    /// rather than adding a runtime branch.
    ///
    /// ```compile_fail
    /// use std::pin::Pin;
    ///
    /// use dope::manifold::Manifold;
    /// use dope::manifold::typed::TypedToken;
    /// use dope::{DriverContext, Token};
    ///
    /// struct Outer;
    /// struct Inner;
    ///
    /// impl<'d> Manifold<'d> for Outer {
    ///     const ID: u8 = 1;
    ///     fn pre_park(self: Pin<&mut Self>, _: &mut DriverContext<'_, 'd>) {}
    /// }
    ///
    /// impl<'d> Manifold<'d> for Inner {
    ///     const ID: u8 = 2;
    ///     fn pre_park(self: Pin<&mut Self>, _: &mut DriverContext<'_, 'd>) {}
    /// }
    ///
    /// fn forward<'d>(token: TypedToken<Outer>) -> TypedToken<Inner> {
    ///     token.retag::<'d, Inner>()
    /// }
    /// ```
    pub const fn retag<'d, N>(self) -> TypedToken<N>
    where
        M: Manifold<'d>,
        N: Manifold<'d>,
    {
        const {
            assert!(
                M::ID == N::ID,
                "cannot retag a token across different manifold routes"
            )
        };
        TypedToken(self.0, PhantomData)
    }

    pub const fn into_inner(self) -> Token {
        self.0
    }
    pub const fn slot(self) -> SlotIndex {
        self.0.slot()
    }
}
