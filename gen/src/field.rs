pub(crate) struct Field {
    pub(crate) name: syn::Ident,
    pub(crate) ty: syn::Type,
    pub(crate) optional: bool,
    pub(crate) control: bool,
    pub(crate) borrowed: bool,
    pub(crate) pinned: bool,
    pub(crate) const_ident: syn::Ident,
}

impl Field {
    pub(crate) fn manifold_ty(&self) -> proc_macro2::TokenStream {
        let ty = if self.borrowed {
            Self::argument(&self.ty, "Anchor")
        } else if self.optional {
            Self::argument(&self.ty, "Option")
        } else {
            &self.ty
        };
        quote::quote! { #ty }
    }

    pub(crate) fn wrap_body(
        &self,
        runtime: &syn::Path,
        body_with: impl FnOnce(proc_macro2::TokenStream) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let field = &self.name;
        let body = body_with(quote::quote! { __m });
        if self.optional {
            quote::quote! {{
                let __f = __app.as_mut().project().#field;
                if let ::core::option::Option::Some(__m) = __f.as_pin_mut() {
                    #body
                }
            }}
        } else if self.borrowed {
            quote::quote! {{
                let __f = __app.as_mut().project().#field;
                let __m = #runtime::client::Anchor::as_mut(__f);
                #body
            }}
        } else {
            quote::quote! {{
                let __m = __app.as_mut().project().#field;
                #body
            }}
        }
    }

    pub(crate) fn wrap_body_ref(
        &self,
        runtime: &syn::Path,
        body_with: impl FnOnce(proc_macro2::TokenStream) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let field = &self.name;
        let body = body_with(quote::quote! { __m });
        if self.optional {
            quote::quote! {{
                let __f = __app.project_ref().#field;
                if let ::core::option::Option::Some(__m) = __f.as_pin_ref() {
                    #body
                }
            }}
        } else if self.borrowed {
            quote::quote! {{
                let __f = __app.project_ref().#field;
                let __m = #runtime::client::Anchor::as_ref(__f);
                #body
            }}
        } else {
            quote::quote! {{
                let __m = __app.project_ref().#field;
                #body
            }}
        }
    }

    fn argument<'a>(ty: &'a syn::Type, wrapper: &str) -> &'a syn::Type {
        use syn::{GenericArgument, PathArguments, Type};

        if let Type::Path(path) = ty
            && let Some(segment) = path.path.segments.last()
            && segment.ident == wrapper
            && let PathArguments::AngleBracketed(arguments) = &segment.arguments
            && let Some(GenericArgument::Type(inner)) = arguments.args.last()
        {
            return inner;
        }
        ty
    }
}
