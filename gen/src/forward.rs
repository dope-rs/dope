use syn::parse;

use crate::{attributes, brand};

pub(super) struct Forward(syn::DeriveInput);

struct Spec {
    field: syn::Ident,
    field_ty: syn::Type,
    brand: brand::Brand,
    capability: Option<syn::Type>,
}

struct Arguments {
    lifetime: Option<syn::Lifetime>,
    capability: Option<syn::Type>,
}

impl parse::Parse for Arguments {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let mut lifetime = None;
        let mut capability = None;
        while !input.is_empty() {
            if input.peek(syn::Lifetime) {
                if lifetime.replace(input.parse()?).is_some() {
                    return Err(input.error("forward accepts one driver lifetime"));
                }
            } else {
                let name: syn::Ident = input.parse()?;
                if name != "capability" {
                    return Err(syn::Error::new_spanned(
                        name,
                        "expected `capability = Type`",
                    ));
                }
                input.parse::<syn::Token![=]>()?;
                if capability.replace(input.parse()?).is_some() {
                    return Err(input.error("forward accepts one capability type"));
                }
            }
            if input.is_empty() {
                break;
            }
            input.parse::<syn::Token![,]>()?;
        }
        Ok(Self {
            lifetime,
            capability,
        })
    }
}

impl Forward {
    pub(crate) fn new(input: syn::DeriveInput) -> Self {
        Self(input)
    }

    pub(crate) fn expand(self) -> proc_macro::TokenStream {
        use quote::quote;

        let input = self.0;
        let spec = match Self::parse(&input) {
            Ok(spec) => spec,
            Err(error) => return error.to_compile_error().into(),
        };
        let name = &input.ident;
        let (_, ty_generics, where_clause) = input.generics.split_for_impl();
        let field = &spec.field;
        let field_ty = &spec.field_ty;
        let lifetime = &spec.brand.lifetime;
        let dispatch = spec.capability.as_ref().map_or_else(
            || quote! { <#field_ty as ::dope::manifold::dispatch::raw::Manifold<#lifetime>>::Dispatch },
            |capability| quote! { #capability },
        );
        let activate = spec.capability.as_ref().map_or_else(
            || quote! { <#field_ty as ::dope::manifold::dispatch::raw::Manifold<#lifetime>>::Activate },
            |capability| quote! { #capability },
        );
        let pre_park = spec.capability.as_ref().map_or_else(
            || quote! { <#field_ty as ::dope::manifold::dispatch::raw::Manifold<#lifetime>>::PrePark },
            |capability| quote! { #capability },
        );
        let shutdown = spec.capability.as_ref().map_or_else(
            || quote! { <#field_ty as ::dope::manifold::dispatch::raw::Manifold<#lifetime>>::Shutdown },
            |capability| quote! { #capability },
        );
        let binder = &spec.brand.binder;
        let impl_generics = {
            let mut params = input.generics.params.clone();
            for parameter in &mut params {
                match parameter {
                    syn::GenericParam::Type(parameter) => parameter.default = None,
                    syn::GenericParam::Const(parameter) => parameter.default = None,
                    syn::GenericParam::Lifetime(_) => {}
                }
            }
            let params = params.iter();
            quote! { <#binder #(#params),*> }
        };

        quote! {
            unsafe impl #impl_generics ::dope::manifold::dispatch::raw::Manifold<#lifetime> for #name #ty_generics
            #where_clause
            {
                const ID: u8 = <#field_ty as ::dope::manifold::dispatch::raw::Manifold<#lifetime>>::ID;
                type Dispatch = #dispatch;
                type Activate = #activate;
                type PrePark = #pre_park;
                type Shutdown = #shutdown;
                fn install(
                    self: ::core::pin::Pin<&mut Self>,
                    install: &mut ::dope::core::driver::lifecycle::Install<'_, #lifetime>,
                ) {
                    let __field = self.project().#field;
                    ::dope::manifold::dispatch::raw::Manifold::install(__field, install)
                }
                unsafe fn dispatch<'__turn>(
                    self: ::core::pin::Pin<&mut Self>,
                    ev: ::dope::core::io::Event<#lifetime>,
                    turn: ::dope::core::driver::schedule::Turn<'__turn, #lifetime>,
                    driver: &mut ::dope::manifold::dispatch::raw::Context<'_, '_, #lifetime, Self::Dispatch>,
                ) -> ::core::ops::ControlFlow<::dope::core::io::Event<#lifetime>> {
                    let _ = <#field_ty as ::dope::manifold::dispatch::raw::Manifold<#lifetime>>::ID;
                    let __field = self.project().#field;
                    // SAFETY: Forward::dispatch inherits the installed-owner precondition.
                    unsafe { ::dope::manifold::dispatch::raw::Manifold::dispatch(__field, ev, turn, driver) }
                }
                unsafe fn pre_park<'__turn>(
                    self: ::core::pin::Pin<&mut Self>,
                    turn: ::dope::core::driver::schedule::Turn<'__turn, #lifetime>,
                    driver: &mut ::dope::manifold::dispatch::raw::Context<'_, '_, #lifetime, Self::PrePark>,
                ) {
                    let __field = self.project().#field;
                    // SAFETY: Forward::pre_park inherits the installed-owner precondition.
                    unsafe { ::dope::manifold::dispatch::raw::Manifold::pre_park(__field, turn, driver) }
                }
                fn progress(
                    self: ::core::pin::Pin<&Self>,
                    region: &::o3::cell::region::Token<#lifetime>,
                ) -> ::dope::core::driver::schedule::Progress<#lifetime> {
                    let __field = self.project_ref().#field;
                    ::dope::manifold::dispatch::raw::Manifold::progress(__field, region)
                }
                fn shutdown_progress(
                    self: ::core::pin::Pin<&Self>,
                    region: &::o3::cell::region::Token<#lifetime>,
                ) -> ::dope::core::driver::schedule::Progress<#lifetime> {
                    let __field = self.project_ref().#field;
                    ::dope::manifold::dispatch::raw::Manifold::shutdown_progress(__field, region)
                }
                unsafe fn activate<'__turn>(
                    self: ::core::pin::Pin<&mut Self>,
                    target: ::dope::manifold::dispatch::typed::Token<#lifetime, Self>,
                    turn: ::dope::core::driver::schedule::Turn<'__turn, #lifetime>,
                    driver: &mut ::dope::manifold::dispatch::raw::Context<'_, '_, #lifetime, Self::Activate>,
                ) {
                    let __typed = target.retag::<#field_ty>();
                    let __field = self.project().#field;
                    // SAFETY: Forward::activate inherits the installed-owner precondition.
                    unsafe { ::dope::manifold::dispatch::raw::Manifold::activate(__field, __typed, turn, driver) }
                }
                fn shutdown<'__turn>(
                    self: ::core::pin::Pin<&mut Self>,
                    turn: ::dope::core::driver::schedule::Turn<'__turn, #lifetime>,
                    context: &mut ::dope::manifold::dispatch::raw::Context<'_, '_, #lifetime, Self::Shutdown>,
                ) {
                    let __field = self.project().#field;
                    ::dope::manifold::dispatch::raw::Manifold::shutdown(
                        __field,
                        turn,
                        context,
                    );
                }
                fn finish(
                    self: ::core::pin::Pin<&mut Self>,
                    finish: &mut ::dope::core::driver::lifecycle::Finalize<'_, #lifetime>,
                ) {
                    let __field = self.project().#field;
                    ::dope::manifold::dispatch::raw::Manifold::finish(__field, finish)
                }
            }
        }
        .into()
    }

    fn parse(input: &syn::DeriveInput) -> Result<Spec, syn::Error> {
        use syn::{Data, Error, Fields, Meta};

        use crate::brand::Brand;
        attributes::Attributes::reject_packed(input.attrs.as_slice())?;
        let name = &input.ident;
        let data = match &input.data {
            Data::Struct(s) => s,
            _ => {
                return Err(Error::new_spanned(name, "Forward requires a struct"));
            }
        };
        let fields = match &data.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return Err(Error::new_spanned(name, "Forward requires named fields"));
            }
        };
        let mut fields = fields.iter();
        let Some(marked_field) = fields.next() else {
            return Err(Error::new_spanned(
                name,
                "Forward requires exactly one field",
            ));
        };
        if let Some(extra) = fields.next() {
            return Err(Error::new_spanned(
                extra,
                "Forward requires exactly one field",
            ));
        }
        let Some(field) = &marked_field.ident else {
            return Err(Error::new_spanned(
                marked_field,
                "Forward requires a named field",
            ));
        };
        let mut attrs = marked_field
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("forward"));
        let Some(attr) = attrs.next() else {
            return Err(Error::new_spanned(
                field,
                "Forward field must be marked `#[forward]`",
            ));
        };
        if let Some(extra) = attrs.next() {
            return Err(Error::new_spanned(
                extra,
                "Forward field accepts one `#[forward]` attribute",
            ));
        }
        if !marked_field
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("pin"))
        {
            return Err(Error::new_spanned(
                field,
                "`#[forward]` field must be marked `#[pin]`",
            ));
        }
        let (brand, capability) = match &attr.meta {
            Meta::Path(_) => (Brand::infer(&input.generics), None),
            Meta::List(_) => {
                let arguments = attr.parse_args::<Arguments>()?;
                let brand = match arguments.lifetime {
                    Some(lifetime) => Brand::explicit(&input.generics, lifetime)?,
                    None => Brand::infer(&input.generics),
                };
                (brand, arguments.capability)
            }
            Meta::NameValue(_) => {
                return Err(Error::new_spanned(
                    attr,
                    "expected `#[forward]`, `#[forward('d)]`, or `#[forward('d, capability = Type)]`",
                ));
            }
        };
        Ok(Spec {
            field: field.clone(),
            field_ty: marked_field.ty.clone(),
            brand,
            capability,
        })
    }
}
