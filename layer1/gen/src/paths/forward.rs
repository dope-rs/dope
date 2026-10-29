use proc_macro2::TokenStream;

use crate::paths::package::Package;

pub(crate) struct Forward {
    core: TokenStream,
    manifold: TokenStream,
}

impl Forward {
    pub(crate) fn resolve(facade: Option<TokenStream>) -> Self {
        let root = Package::Dope.root(facade);
        let core = root.core();
        let manifold = root.manifold();
        Self { core, manifold }
    }

    pub(crate) fn core(&self) -> &TokenStream {
        &self.core
    }

    pub(crate) fn manifold(&self) -> &TokenStream {
        &self.manifold
    }
}
