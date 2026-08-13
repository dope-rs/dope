use crate::driver::route;

/// A payload carrying the affine driver target authorized to receive it.
pub(in crate::io) struct Targeted<T> {
    token: route::Token,
    value: T,
}

impl<T> Targeted<T> {
    pub(super) const fn new(token: route::Token, value: T) -> Self {
        Self { token, value }
    }

    pub(super) const fn token(&self) -> route::Token {
        self.token
    }

    pub(super) const fn value(&self) -> &T {
        &self.value
    }

    pub(super) fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub(super) fn into_parts(self) -> (route::Token, T) {
        (self.token, self.value)
    }

    pub(super) const fn into_copy_parts(self) -> (route::Token, T)
    where
        T: Copy,
    {
        (self.token, self.value)
    }

    pub(super) fn map<U>(self, map: impl FnOnce(T) -> U) -> Targeted<U> {
        Targeted::new(self.token, map(self.value))
    }
}
