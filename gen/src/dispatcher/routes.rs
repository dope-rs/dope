use crate::dispatcher;

pub(super) struct Routes<'a> {
    application: &'a dispatcher::Application,
}

impl<'a> Routes<'a> {
    pub(super) fn new(application: &'a dispatcher::Application) -> Self {
        Self { application }
    }

    pub(super) fn generate(&self) -> proc_macro2::TokenStream {
        let application = self.application;
        if application.generics.type_params().next().is_some()
            && application.generics.lifetimes().next().is_none()
        {
            return quote::quote! {};
        }
        let manifold = &application.paths.manifold;
        let name = &application.name;
        let has_lifetime = application.generics.lifetimes().next().is_some();
        let mut bounded_generics = application.generics.clone();
        if has_lifetime {
            let lifetime = &application.brand.lifetime;
            for field in &application.fields {
                let inner = field.manifold_ty();
                bounded_generics.make_where_clause().predicates.push(
                    syn::parse_quote! { #inner: #manifold::dispatch::raw::Manifold<#lifetime> },
                );
            }
        }
        let (impl_generics, ty_generics, where_clause) = bounded_generics.split_for_impl();
        let consts = application.fields.iter().map(|field| {
            let const_name = &field.const_ident;
            let inner = field.manifold_ty();
            let route = if has_lifetime {
                let lifetime = &application.brand.lifetime;
                quote::quote! { <#inner as #manifold::dispatch::raw::Manifold<#lifetime>>::ID }
            } else {
                quote::quote! { <#inner as #manifold::dispatch::raw::Manifold>::ID }
            };
            quote::quote! {
                pub const #const_name: u8 = #route;
            }
        });
        let uniqueness = self.uniqueness();
        quote::quote! {
            impl #impl_generics #name #ty_generics #where_clause {
                #(#consts)*
                #uniqueness
            }
        }
    }

    fn uniqueness(&self) -> proc_macro2::TokenStream {
        let application = self.application;
        if application.fields.len() < 2 {
            return quote::quote! {};
        }
        let count = application.fields.len();
        let manifold = &application.paths.manifold;
        let ids = application.fields.iter().map(|field| {
            let inner = field.manifold_ty();
            quote::quote! { <#inner as #manifold::dispatch::raw::Manifold>::ID }
        });
        quote::quote! {
            #[doc(hidden)]
            pub const __MANIFOLD_ID_UNIQUE: () = {
                let __ids: [u8; #count] = [ #(#ids),* ];
                let mut __i = 0;
                while __i < __ids.len() {
                    let mut __j = __i + 1;
                    while __j < __ids.len() {
                        if __ids[__i] == __ids[__j] {
                            ::core::panic!(
                                "Application: duplicate Manifold::ID detected across fields"
                            );
                        }
                        __j += 1;
                    }
                    __i += 1;
                }
            };
        }
    }
}
