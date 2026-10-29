use std::marker;

use dope_core::driver::route::{Identity, Tag};
use dope_runtime::client;

/// Builds a client from an exact driver-owned endpoint root.
pub trait Provider<'life, 'd: 'life, ID: Tag, const MAX: usize>: 'life {
    type Client<'app, P: ?Sized + 'app>: 'app
    where
        'life: 'app,
        'd: 'app;

    fn provide<'app, P: ?Sized + 'app>(
        &'life self,
        bound: client::Bound<'app, 'd, P>,
    ) -> Self::Client<'app, P>
    where
        'life: 'app,
        'd: 'app;
}

pub struct State<'life, 'd: 'life, P: ?Sized, L, ID: Identity, const MAX: usize> {
    pub(super) root: &'life P,
    pub(super) logic: L,
    driver: marker::PhantomData<fn(o3::base::cell::region::Identity<'d>) -> ID>,
}

impl<'life, 'd: 'life, P: ?Sized, L, ID: Identity, const MAX: usize>
    State<'life, 'd, P, L, ID, MAX>
{
    pub(super) const fn new(root: &'life P, logic: L) -> Self {
        Self {
            root,
            logic,
            driver: marker::PhantomData,
        }
    }

    pub const fn root(&self) -> &'life P {
        self.root
    }

    pub const fn logic(&self) -> &L {
        &self.logic
    }

    pub fn logic_mut(&mut self) -> &mut L {
        &mut self.logic
    }
}
