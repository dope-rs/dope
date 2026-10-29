use crate::app::Spec;

pub(crate) fn generate(app: &Spec) -> proc_macro2::TokenStream {
    let [vdriver, vregion, vturn] =
        ["__driver", "__region", "__turn"].map(|name| app.brand.ident(name));
    let Some(owner) = app.drain() else {
        return quote::quote! {};
    };
    let name = &app.name;
    let field = owner.name();
    let inner = owner.inner();
    let core = app.paths.core();
    let runtime = app.paths.runtime();
    let lifetime = app.brand.lifetime();
    let args = app.generics.split_for_impl().1;
    let mut generics = app.brand.generics(&app.generics);
    let bounds = &mut generics.make_where_clause().predicates;
    bounds.push(syn::parse_quote!(#name #args: #runtime::executor::raw::App<#lifetime>));
    bounds.push(syn::parse_quote!(#inner: #runtime::executor::raw::Drain<#lifetime>));
    let (head, _, clause) = generics.split_for_impl();
    quote::quote! {
        /// SAFETY: the parser admits one required drain owner. Both mutations
        /// forward its pinned projection; all other owners remain installed.
        unsafe impl #head #runtime::executor::raw::Drain<#lifetime>
            for #name #args #clause
        {
            fn begin(
                self: ::core::pin::Pin<&mut Self>,
                #vturn: #core::driver::schedule::Turn<'_, #lifetime>,
                #vdriver: &mut #core::driver::retained::Context<'_, '_, #lifetime>,
            ) {
                #runtime::executor::raw::Drain::begin(
                    self.project().#field,
                    #vturn,
                    #vdriver,
                );
            }

            fn progress(
                self: ::core::pin::Pin<&Self>,
                #vregion: &#core::o3::base::cell::region::Token<
                    #core::o3::base::cell::region::Identity<#lifetime>,
                >,
            ) -> #core::driver::schedule::Progress<#lifetime> {
                #runtime::executor::raw::Drain::progress(
                    self.project_ref().#field,
                    #vregion,
                )
            }

            fn force(
                self: ::core::pin::Pin<&mut Self>,
                #vturn: #core::driver::schedule::Turn<'_, #lifetime>,
                #vdriver: &mut #core::driver::retained::Context<'_, '_, #lifetime>,
            ) {
                #runtime::executor::raw::Drain::force(
                    self.project().#field,
                    #vturn,
                    #vdriver,
                );
            }
        }
    }
}
