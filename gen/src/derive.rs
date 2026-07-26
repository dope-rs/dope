use proc_macro2::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Attribute, Error, Generics, Meta, Token};

pub(crate) trait DeriveAttrs {
    fn reject_packed(&self) -> Result<(), Error>;
}

impl DeriveAttrs for [Attribute] {
    fn reject_packed(&self) -> Result<(), Error> {
        for attr in self {
            if !attr.path().is_ident("repr") {
                continue;
            }
            let reprs = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            if let Some(repr) = reprs.iter().find(|repr| match repr {
                Meta::Path(path) => path.is_ident("packed"),
                Meta::List(list) => list.path.is_ident("packed"),
                Meta::NameValue(value) => value.path.is_ident("packed"),
            }) {
                return Err(Error::new_spanned(
                    repr,
                    "pinned projection does not support repr(packed)",
                ));
            }
        }
        Ok(())
    }
}

pub(crate) trait DeriveGenerics {
    fn brand_lifetime(&self) -> (TokenStream, TokenStream);
}

impl DeriveGenerics for Generics {
    fn brand_lifetime(&self) -> (TokenStream, TokenStream) {
        match self.lifetimes().next() {
            Some(lt) => {
                let lt = &lt.lifetime;
                (quote! { #lt }, quote! {})
            }
            None => (quote! { '__d }, quote! { '__d, }),
        }
    }
}
