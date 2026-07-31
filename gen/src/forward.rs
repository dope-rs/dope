use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Error, Fields, Ident, Lifetime, Meta, Type};

use crate::derive::{DeriveAttrs, DeriveGenerics};

pub(crate) struct Forward(DeriveInput);

impl Forward {
    pub(crate) fn new(input: DeriveInput) -> Self {
        Self(input)
    }

    pub(crate) fn expand(self) -> TokenStream {
        let input = self.0;
        if let Err(error) = input.attrs.reject_packed() {
            return error.to_compile_error().into();
        }
        let name = &input.ident;
        let (_, ty_generics, where_clause) = input.generics.split_for_impl();
        let data = match &input.data {
            Data::Struct(s) => s,
            _ => {
                return Error::new_spanned(name, "Forward requires a struct")
                    .to_compile_error()
                    .into();
            }
        };
        let fields = match &data.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return Error::new_spanned(name, "Forward requires named fields")
                    .to_compile_error()
                    .into();
            }
        };
        let mut marked: Vec<(&Ident, &Type, &Attribute)> = Vec::new();
        for f in fields {
            if let Some(attr) = f.attrs.iter().find(|a| a.path().is_ident("forward"))
                && let Some(ident) = &f.ident
            {
                marked.push((ident, &f.ty, attr));
            }
        }
        let (field, field_ty, attr) = match marked.as_slice() {
            [one] => *one,
            [] => {
                return Error::new_spanned(
                    name,
                    "Forward needs exactly one field marked `#[forward]`",
                )
                .to_compile_error()
                .into();
            }
            _ => {
                return Error::new_spanned(name, "Forward accepts only one `#[forward]` field")
                    .to_compile_error()
                    .into();
            }
        };
        let (brand, fresh) = match &attr.meta {
            Meta::Path(_) => input.generics.brand_lifetime(),
            Meta::List(_) => {
                let brand = match attr.parse_args::<Lifetime>() {
                    Ok(brand) => brand,
                    Err(error) => return error.to_compile_error().into(),
                };
                if !input
                    .generics
                    .lifetimes()
                    .any(|lifetime| lifetime.lifetime == brand)
                {
                    return Error::new_spanned(brand, "forward lifetime is not a type parameter")
                        .to_compile_error()
                        .into();
                }
                (quote! { #brand }, quote! {})
            }
            Meta::NameValue(_) => {
                return Error::new_spanned(attr, "expected `#[forward]` or `#[forward('d)]`")
                    .to_compile_error()
                    .into();
            }
        };
        let impl_generics = {
            let params = input.generics.params.iter();
            quote! { <#fresh #(#params),*> }
        };

        quote! {
            impl #impl_generics ::dope::manifold::Manifold<#brand> for #name #ty_generics
            #where_clause
            {
                const ID: u8 = <#field_ty as ::dope::manifold::Manifold<#brand>>::ID;
                fn dispatch(
                    self: ::core::pin::Pin<&mut Self>,
                    ev: ::dope::Event<#brand>,
                    driver: &mut ::dope::DriverContext<'_, #brand>,
                ) {
                    let _ = <#field_ty as ::dope::manifold::Manifold<#brand>>::ID;
                    let __field = self.project().#field;
                    ::dope::manifold::Manifold::dispatch(__field, ev, driver)
                }
                fn pre_park(
                    self: ::core::pin::Pin<&mut Self>,
                    driver: &mut ::dope::DriverContext<'_, #brand>,
                ) {
                    let __field = self.project().#field;
                    ::dope::manifold::Manifold::pre_park(__field, driver)
                }
                fn idle(self: ::core::pin::Pin<&Self>) -> ::dope::runtime::dispatcher::Idle {
                    let __field = self.project_ref().#field;
                    ::dope::manifold::Manifold::idle(__field)
                }
                fn activate(
                    self: ::core::pin::Pin<&mut Self>,
                    target: ::dope::manifold::typed::TypedToken<Self>,
                    driver: &mut ::dope::DriverContext<'_, #brand>,
                ) {
                    let __typed = target.retag::<#brand, #field_ty>();
                    let __field = self.project().#field;
                    ::dope::manifold::Manifold::activate(__field, __typed, driver)
                }
                fn shutdown(
                    self: ::core::pin::Pin<&mut Self>,
                    driver: &mut ::dope::DriverContext<'_, #brand>,
                ) {
                    let __field = self.project().#field;
                    ::dope::manifold::Manifold::shutdown(__field, driver)
                }
                fn finish(
                    self: ::core::pin::Pin<&mut Self>,
                    context: &mut ::dope::runtime::dispatcher::FinishContext<'_, #brand>,
                ) {
                    let __field = self.project().#field;
                    ::dope::manifold::Manifold::finish(__field, context)
                }
            }
        }
        .into()
    }
}
