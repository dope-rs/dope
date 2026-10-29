use crate::app::{Owner, Spec};

pub(super) fn layout(app: &Spec) -> syn::Generics {
    let runtime = app.paths.runtime();
    let lifetime = app.brand.lifetime();
    let mut generics = app.generics.clone();
    for owner in app.owners.iter().filter(|owner| owner.is_app()) {
        let inner = owner.inner();
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote! { #inner: #runtime::executor::raw::Layout<#lifetime> });
    }
    generics
}

pub(super) fn lifecycle(app: &Spec) -> syn::Generics {
    let manifold = app.paths.manifold();
    let lifetime = app.brand.lifetime();
    let mut generics = app.generics.clone();
    for state in app.states.iter().filter(|state| state.is_scheduled()) {
        let ty = state.ty();
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote! { #ty: #manifold::timing::Schedule<#lifetime> });
    }
    for owner in &app.owners {
        push_owner(&mut generics, app, owner);
    }
    generics
}

pub(super) fn routes(app: &Spec) -> syn::Generics {
    let mut generics = app.generics.clone();
    for owner in app.owners.iter().filter(|owner| owner.is_manifold()) {
        push_owner(&mut generics, app, owner);
    }
    generics
}

fn push_owner(generics: &mut syn::Generics, app: &Spec, owner: &Owner) {
    let manifold = app.paths.manifold();
    let runtime = app.paths.runtime();
    let lifetime = app.brand.lifetime();
    let inner = owner.inner();
    let bound = if owner.is_app() {
        syn::parse_quote! { #inner: #runtime::executor::raw::App<#lifetime> }
    } else {
        syn::parse_quote! {
            #inner: #manifold::app::dispatch::raw::Manifold<#lifetime>
        }
    };
    generics.make_where_clause().predicates.push(bound);
}
