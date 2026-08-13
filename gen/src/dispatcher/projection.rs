use crate::dispatcher;

pub(super) struct Projection<'a> {
    spec: &'a dispatcher::Application,
}

impl<'a> Projection<'a> {
    pub(super) fn new(spec: &'a dispatcher::Application) -> Self {
        Self { spec }
    }

    pub(super) fn generate(self) -> proc_macro2::TokenStream {
        let spec = self.spec;
        let name = &spec.name;
        let (impl_generics, ty_generics, where_clause) = spec.generics.split_for_impl();
        let methods = spec
            .fields
            .iter()
            .filter(|field| field.pinned)
            .map(|field| {
                let ty = &field.ty;
                let field = &field.name;
                let ref_name = quote::format_ident!("{}_ref", field);
                quote::quote! {
                    #[doc(hidden)]
                    pub fn #ref_name(
                        self: ::core::pin::Pin<&Self>,
                    ) -> ::core::pin::Pin<&#ty> {
                        self.project_ref().#field
                    }
                }
            });
        quote::quote! {
            impl #impl_generics #name #ty_generics #where_clause {
                #(#methods)*
            }
        }
    }
}
