use proc_macro2::TokenStream;

use crate::paths::package::Package;

pub(crate) struct Fiber {
    abi: TokenStream,
    brand: TokenStream,
}

impl Fiber {
    pub(crate) fn resolve(facade: Option<TokenStream>) -> Self {
        let root = Package::Fiber.root(facade);
        let abi = root.abi();
        let brand = quote::quote!(#abi::future::raw::Brand);
        Self { abi, brand }
    }

    pub(crate) fn abi(&self) -> &TokenStream {
        &self.abi
    }

    pub(crate) fn brand(&self) -> &TokenStream {
        &self.brand
    }
}
