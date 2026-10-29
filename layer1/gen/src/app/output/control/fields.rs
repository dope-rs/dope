use crate::{
    Failure,
    app::{Spec, output::control::members},
    failure::App,
};

pub(super) fn emit(
    spec: &Spec,
    module: &syn::Ident,
    helper: &syn::Ident,
    seal: &syn::Ident,
    root: &syn::Ident,
) -> Result<(proc_macro2::TokenStream, proc_macro2::TokenStream), Failure> {
    let name = &spec.name;
    let manifold = spec.paths.manifold();
    let runtime = spec.paths.runtime();
    let lifetime = spec.brand.lifetime();
    let args = spec.generics.split_for_impl().1;
    let step = spec.brand.fresh("__coordinate_step");
    let mut generics = spec.brand.generics(&spec.generics);
    generics.params.insert(0, syn::parse_quote!(#step));
    generics
        .make_where_clause()
        .predicates
        .push(syn::parse_quote! { #lifetime: #step });
    generics
        .make_where_clause()
        .predicates
        .extend(members::iter(spec).map(|member| member.bound(manifold, lifetime, &step)));
    let params = &generics.params;
    let clause = &generics.where_clause;
    let (head, helper_args, _) = generics.split_for_impl();
    let fields = members::iter(spec).map(|member| member.declaration(manifold, lifetime, &step));

    let declaration = quote::quote! {
        mod #seal {
            pub struct Seal<T>(::core::marker::PhantomData<fn(T) -> T>);

            pub(super) const fn new<T>() -> Seal<T> {
                Seal(::core::marker::PhantomData)
            }
        }

        pub struct #helper<#params> #clause {
            #(pub #fields,)*
            pub step: #runtime::control::Step<#step, #lifetime>,
            #root: #seal::Seal<#name #args>,
        }

        impl #head #runtime::control::Fields<#step, #lifetime> for #name #args #clause {
            type View = #helper #helper_args;
        }
    };

    let runtime_path =
        syn::parse2::<syn::Path>(runtime.clone()).map_err(|source| Failure::Syntax {
            source,
            kind: App::CoordinateHeader.into(),
        })?;
    check_path(&runtime_path)?;
    let mut alias = generics.clone();
    alias.where_clause = None;
    for param in &mut alias.params {
        match param {
            syn::GenericParam::Lifetime(param) => {
                param.colon_token = None;
                param.bounds.clear();
            }
            syn::GenericParam::Type(param) => {
                param.colon_token = None;
                param.bounds.clear();
                param.default = None;
            }
            syn::GenericParam::Const(param) => {
                let syn::Type::Path(ty) = &param.ty else {
                    return Err(Failure::new(&param.ty, App::CoordinateHeader));
                };
                if ty.qself.is_some() {
                    return Err(Failure::new(&param.ty, App::CoordinateHeader));
                }
                check_path(&ty.path)?;
                param.default = None;
            }
        }
    }
    let alias_params = &alias.params;
    let alias = quote::quote! {
        mod #module {
            use super::*;

            pub(super) type Fields<#alias_params> =
                <super::#name #args as #runtime::control::Fields<#step, #lifetime>>::View;
        }
    };
    Ok((alias, declaration))
}

fn check_path(path: &syn::Path) -> Result<(), Failure> {
    if path
        .segments
        .iter()
        .any(|part| !matches!(part.arguments, syn::PathArguments::None))
        || path.leading_colon.is_none()
            && path.segments.first().is_some_and(|part| {
                part.ident == "self" || part.ident == "super" || part.ident == "Fields"
            })
    {
        return Err(Failure::new(path, App::CoordinateHeader));
    }
    Ok(())
}
