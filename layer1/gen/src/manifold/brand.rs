pub(crate) struct Brand {
    lifetime: syn::Lifetime,
    added: bool,
    source: proc_macro2::TokenStream,
}

pub(crate) enum Inference {
    Selected(Brand),
    Ambiguous,
}

impl Brand {
    pub(crate) fn infer(generics: &syn::Generics, source: &proc_macro2::TokenStream) -> Inference {
        let mut life = generics.lifetimes();
        let Some(param) = life.next() else {
            return Inference::Selected(Self {
                lifetime: crate::tokens::lifetime(source, "__d"),
                added: true,
                source: source.clone(),
            });
        };
        if life.next().is_some() {
            Inference::Ambiguous
        } else {
            Inference::Selected(Self {
                lifetime: param.lifetime.clone(),
                added: false,
                source: source.clone(),
            })
        }
    }

    pub(crate) fn explicit(
        generics: &syn::Generics,
        source: &proc_macro2::TokenStream,
        selected: &syn::Ident,
    ) -> Result<Self, Failure> {
        let Some(param) = generics
            .lifetimes()
            .find(|param| param.lifetime.ident == *selected)
        else {
            return Err(Failure::new(selected, Manifold::Lifetime));
        };
        Ok(Self {
            lifetime: param.lifetime.clone(),
            added: false,
            source: source.clone(),
        })
    }

    pub(crate) fn lifetime(&self) -> &syn::Lifetime {
        &self.lifetime
    }

    pub(crate) fn fresh(&self, name: &str) -> syn::Lifetime {
        crate::tokens::lifetime(&self.source, name)
    }

    pub(crate) fn ident(&self, name: &str) -> syn::Ident {
        crate::tokens::ident(&self.source, name)
    }

    pub(crate) fn generics(&self, source: &syn::Generics) -> syn::Generics {
        let mut generics = source.clone();
        let lifetime = &self.lifetime;
        if self.added {
            generics.params.insert(0, syn::parse_quote!(#lifetime));
        }
        generics
    }
}
use crate::{Failure, failure::Manifold};
