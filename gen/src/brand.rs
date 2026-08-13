pub(crate) struct Brand {
    pub(crate) lifetime: proc_macro2::TokenStream,
    pub(crate) binder: proc_macro2::TokenStream,
}

impl Brand {
    pub(crate) fn infer(generics: &syn::Generics) -> Self {
        match generics.lifetimes().next() {
            Some(lifetime) => {
                let lifetime = &lifetime.lifetime;
                Self {
                    lifetime: quote::quote! { #lifetime },
                    binder: quote::quote! {},
                }
            }
            None => Self {
                lifetime: quote::quote! { '__d },
                binder: quote::quote! { '__d, },
            },
        }
    }

    pub(crate) fn explicit(
        generics: &syn::Generics,
        lifetime: syn::Lifetime,
    ) -> Result<Self, syn::Error> {
        if !generics
            .lifetimes()
            .any(|parameter| parameter.lifetime == lifetime)
        {
            use syn::Error;
            return Err(Error::new_spanned(
                lifetime,
                "selected driver lifetime is not a type parameter",
            ));
        }
        Ok(Self {
            lifetime: quote::quote! { #lifetime },
            binder: quote::quote! {},
        })
    }
}
