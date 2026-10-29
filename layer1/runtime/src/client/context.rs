use std::pin;

use crate::client::Bound;

/// The exact selected provider and its application lifetime, issued together.
///
/// The short provider borrow cannot escape into a client. Private construction
/// prevents rebuilding this capability with a different provider.
pub struct Context<'borrow, 'app: 'borrow, 'd: 'app, P: ?Sized> {
    provider: pin::Pin<&'borrow P>,
    bound: Bound<'app, 'd, P>,
}

impl<'borrow, 'app: 'borrow, 'd: 'app, P: ?Sized> Context<'borrow, 'app, 'd, P> {
    pub(crate) const fn new(provider: pin::Pin<&'borrow P>) -> Self {
        Self {
            provider,
            bound: Bound::new(),
        }
    }

    pub fn into_parts(self) -> (pin::Pin<&'borrow P>, Bound<'app, 'd, P>) {
        (self.provider, self.bound)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Context<'_, '_, '_, ()>>() == std::mem::size_of::<&()>());
    assert!(!std::mem::needs_drop::<Context<'_, '_, '_, ()>>());
};
