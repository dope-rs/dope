use crate::app::{Owner, Spec, State};

pub(super) fn iter(spec: &Spec) -> impl Iterator<Item = Member<'_>> {
    spec.owners
        .iter()
        .filter(|owner| owner.is_control())
        .map(Member::Control)
        .chain(spec.states.iter().map(Member::State))
}

pub(super) enum Member<'a> {
    Control(&'a Owner),
    State(&'a State),
}

impl Member<'_> {
    pub(super) fn bound(
        &self,
        manifold: &proc_macro2::TokenStream,
        lifetime: &syn::Lifetime,
        step: &syn::Lifetime,
    ) -> syn::WherePredicate {
        match self {
            Self::Control(owner) => {
                let inner = owner.inner();
                syn::parse_quote! {
                    #inner: #manifold::app::dispatch::raw::Control<#lifetime>
                        + #step
                }
            }
            Self::State(state) => {
                let ty = state.ty();
                syn::parse_quote! { #ty: #step }
            }
        }
    }

    pub(super) fn declaration(
        &self,
        manifold: &proc_macro2::TokenStream,
        lifetime: &syn::Lifetime,
        step: &syn::Lifetime,
    ) -> proc_macro2::TokenStream {
        match self {
            Self::Control(owner) => {
                let name = owner.name();
                let inner = owner.inner();
                let control = quote::quote! {
                    <#inner as #manifold::app::dispatch::raw::Control<#lifetime>>::View<#step>
                };
                let ty = if owner.is_optional() {
                    quote::quote! { ::core::option::Option<#control> }
                } else {
                    control
                };
                quote::quote! { #name: #ty }
            }
            Self::State(state) => {
                let name = state.name();
                let ty = state.ty();
                quote::quote! { #name: &#step mut #ty }
            }
        }
    }

    pub(super) fn initializer(&self, spec: &Spec) -> proc_macro2::TokenStream {
        let [vfield, vowner, vproj] =
            ["__field", "__owner", "__projection"].map(|name| spec.brand.ident(name));
        let manifold = spec.paths.manifold();
        let lifetime = spec.brand.lifetime();
        match self {
            Self::Control(owner) => {
                let name = owner.name();
                let inner = owner.inner();
                let control = quote::quote! {
                    // SAFETY: `__owner` is the projection of this exact field,
                    // still pinned by the installed application root for `'d`.
                    unsafe {
                        <#inner as #manifold::app::dispatch::raw::Control<#lifetime>>::root(
                            #vowner,
                        )
                    }
                };
                if owner.is_optional() {
                    quote::quote! {
                        #name: {
                            let #vfield = #vproj.#name;
                            ::core::option::Option::as_pin_mut(#vfield)
                                .map(|#vowner| #control)
                        }
                    }
                } else {
                    quote::quote! {
                        #name: {
                            let #vowner = #vproj.#name;
                            #control
                        }
                    }
                }
            }
            Self::State(state) => {
                let name = state.name();
                quote::quote! { #name: #vproj.#name }
            }
        }
    }
}
