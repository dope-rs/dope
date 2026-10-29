use crate::app::{Spec, output::control::members};

pub(super) fn emit(
    spec: &Spec,
    helper: &syn::Ident,
    seal: &syn::Ident,
    root: &syn::Ident,
) -> proc_macro2::TokenStream {
    let [vapp, vflow, vpark, vproj, vstep] =
        ["__app", "__flow", "__park", "__projection", "__step"].map(|name| spec.brand.ident(name));
    let runtime = spec.paths.runtime();
    let fields = members::iter(spec).map(|member| member.initializer(spec));

    quote::quote! {
        loop {
            let ::core::option::Option::Some(#vstep) = #vpark.next_step() else {
                break;
            };
            let #vproj = #vapp.as_mut().project();
            let #vflow = Self::coordinate(#helper {
                #(#fields,)*
                step: #vstep,
                #root: #seal::new(),
            });
            if ::core::matches!(#vflow, #runtime::control::Flow::Idle) {
                break;
            }
        }
    }
}
