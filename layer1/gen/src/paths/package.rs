use proc_macro2::TokenStream;

pub(super) enum Package {
    Dope,
    Fiber,
}

impl Package {
    pub(super) fn root(self, explicit: Option<TokenStream>) -> Root {
        let path = match explicit {
            Some(path) => path,
            None => match self {
                Self::Dope => quote::quote!(::dope),
                Self::Fiber => quote::quote!(::dope_fiber),
            },
        };
        Root(path)
    }
}

pub(super) struct Root(TokenStream);

impl Root {
    pub(super) fn core(&self) -> TokenStream {
        let root = &self.0;
        quote::quote!(#root::core)
    }

    pub(super) fn manifold(&self) -> TokenStream {
        let root = &self.0;
        quote::quote!(#root::manifold)
    }

    pub(super) fn runtime(&self) -> TokenStream {
        let root = &self.0;
        quote::quote!(#root::runtime)
    }

    pub(super) fn abi(&self) -> TokenStream {
        let root = &self.0;
        quote::quote!(#root::abi)
    }
}
