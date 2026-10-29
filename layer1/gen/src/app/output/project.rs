use crate::app::{Owner, Spec};

pub(crate) struct Project<'a> {
    owner: &'a Owner,
    app: &'a Spec,
}

impl<'a> Project<'a> {
    pub(crate) const fn new(owner: &'a Owner, app: &'a Spec) -> Self {
        Self { owner, app }
    }

    pub(crate) fn map(
        &self,
        keep: proc_macro2::TokenStream,
        build: impl FnOnce(proc_macro2::TokenStream) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let [vapp, vfield, vowner] =
            ["__app", "__field", "__owner"].map(|name| self.app.brand.ident(name));
        let name = self.owner.name();
        let body = build(quote::quote! { #vowner });
        if self.owner.is_optional() {
            quote::quote! {{
                let #vfield = #vapp.as_mut().project().#name;
                match ::core::option::Option::as_pin_mut(#vfield) {
                    ::core::option::Option::Some(#vowner) => { #body }
                    ::core::option::Option::None => { #keep }
                }
            }}
        } else {
            quote::quote! {{
                let #vowner = #vapp.as_mut().project().#name;
                #body
            }}
        }
    }

    pub(crate) fn shared(
        &self,
        build: impl FnOnce(proc_macro2::TokenStream) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let [vapp, vfield, vowner] =
            ["__app", "__field", "__owner"].map(|name| self.app.brand.ident(name));
        let name = self.owner.name();
        let body = build(quote::quote! { #vowner });
        if self.owner.is_optional() {
            quote::quote! {{
                let #vfield = #vapp.project_ref().#name;
                if let ::core::option::Option::Some(#vowner) =
                    ::core::option::Option::as_pin_ref(#vfield)
                {
                    #body
                }
            }}
        } else {
            quote::quote! {{
                let #vowner = #vapp.project_ref().#name;
                #body
            }}
        }
    }
}
