use crate::app::{Spec, output::bounds};

pub(crate) fn generate(app: &Spec) -> proc_macro2::TokenStream {
    let manifold = app.paths.manifold();
    let core = app.paths.core();
    let name = &app.name;
    let lifetime = app.brand.lifetime();
    let generics = app.brand.generics(&bounds::routes(app));
    let (params, _, clause) = generics.split_for_impl();
    let args = app.generics.split_for_impl().1;
    let consts = app
        .owners
        .iter()
        .filter_map(|owner| owner.route().map(|route| (owner, route)))
        .map(|(owner, ident)| {
            let inner = owner.inner();
            quote::quote! {
                pub const #ident: #core::driver::route::Id =
                    <#inner as #manifold::app::dispatch::raw::Manifold<#lifetime>>::ID;
            }
        });
    quote::quote! {
        impl #params #name #args #clause {
            #(#consts)*
        }
    }
}

pub(crate) fn sets(app: &Spec) -> proc_macro2::TokenStream {
    let core = app.paths.core();
    let manifold = app.paths.manifold();
    let runtime = app.paths.runtime();
    let lifetime = app.brand.lifetime();
    let routes = app.owners.iter().map(|owner| {
        let inner = owner.inner();
        if owner.is_app() {
            quote::quote! {
                <#inner as #runtime::executor::raw::App<#lifetime>>::ROUTES
            }
        } else {
            quote::quote! {
                #core::driver::settings::Routes::singleton_id(
                    <#inner as #manifold::app::dispatch::raw::Manifold<#lifetime>>::ID,
                )
            }
        }
    });
    quote::quote! { [#(#routes),*] }
}
