use proc_macro2::TokenStream;

use crate::paths::package::{Package, Root};

pub(crate) struct App {
    core: TokenStream,
    manifold: TokenStream,
    runtime: TokenStream,
}

impl App {
    pub(crate) fn discover() -> Self {
        Self::from_root(Package::Dope.root(None))
    }

    pub(crate) fn from_facade(facade: TokenStream) -> Self {
        Self::from_root(Package::Dope.root(Some(facade)))
    }

    pub(crate) fn from_parts(
        core: TokenStream,
        manifold: TokenStream,
        runtime: TokenStream,
    ) -> Self {
        Self {
            core,
            manifold,
            runtime,
        }
    }

    pub(crate) fn core(&self) -> &TokenStream {
        &self.core
    }

    pub(crate) fn manifold(&self) -> &TokenStream {
        &self.manifold
    }

    pub(crate) fn runtime(&self) -> &TokenStream {
        &self.runtime
    }

    fn from_root(root: Root) -> Self {
        Self {
            core: root.core(),
            manifold: root.manifold(),
            runtime: root.runtime(),
        }
    }
}
