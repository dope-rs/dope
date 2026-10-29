use std::{ops, pin};

use o3::base::cell::region;

use crate::task::storage::scope::{Brand, Scope};

/// Storage whose identities retain the unique brand consumed at construction.
#[pin_project::pin_project(!Unpin)]
pub struct Owned<'s, S> {
    #[pin]
    source: S,
    brand: region::Token<region::Identity<'s>>,
}

impl<'s, S> Owned<'s, S> {
    pub fn new(source: S, brand: Brand<'s>) -> Self {
        Self {
            source,
            brand: brand.into_token(),
        }
    }

    pub fn scope(self: pin::Pin<&mut Self>) -> Scope<'s, pin::Pin<&mut S>> {
        let this = self.project();
        Scope::new(this.source, this.brand.proof())
    }

    pub fn as_pin_ref(self: pin::Pin<&Self>) -> pin::Pin<&S> {
        self.project_ref().source
    }
}

impl<S> ops::Deref for Owned<'_, S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.source
    }
}
