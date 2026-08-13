#![doc = include_str!("compile_fail.md")]
#![warn(unreachable_pub)]

extern crate proc_macro;

pub(crate) mod attributes;
pub(crate) mod brand;
pub(crate) mod dispatcher;
pub(crate) mod fiber;
pub(crate) mod field;
pub(crate) mod forward;
pub(crate) mod lower;

use syn::parse::Parser as _;

use crate::fiber::macros;

fn is_field_path(expression: &syn::Expr) -> bool {
    use syn::Expr;
    match expression {
        Expr::Path(path) => {
            path.qself.is_none()
                && path.path.leading_colon.is_none()
                && path.path.segments.len() == 1
                && matches!(path.path.segments[0].arguments, syn::PathArguments::None)
        }
        Expr::Field(field) => {
            matches!(field.member, syn::Member::Named(_)) && is_field_path(&field.base)
        }
        _ => false,
    }
}

#[proc_macro_attribute]
pub fn fiber_fn(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    use syn::ItemFn;

    let arguments = syn::parse_macro_input!(attr as macros::AttributeArguments);
    let item = syn::parse_macro_input!(input as ItemFn);
    macros::Attribute::new(arguments, item).expand()
}

#[proc_macro]
pub fn fiber(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as macros::Macro);
    input.expand()
}

#[proc_macro_derive(Forward, attributes(forward))]
pub fn forward(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    use forward::Forward;

    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    Forward::new(input).expand()
}

#[proc_macro_derive(Application, attributes(coordinate, dispatcher, manifold))]
pub fn application(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    use dispatcher::Application;

    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    Application::expand(input)
}

#[proc_macro_attribute]
pub fn connector_session(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    use proc_macro2::Span;
    use quote::quote;
    use syn::Error;

    let arguments = {
        use syn::{MetaNameValue, Token, punctuated::Punctuated};

        match Punctuated::<MetaNameValue, Token![,]>::parse_terminated.parse(attr) {
            Ok(arguments) => arguments,
            Err(error) => return error.to_compile_error().into(),
        }
    };
    let mut codec: Option<syn::Expr> = None;
    let mut io: Option<syn::Expr> = None;
    for argument in arguments {
        let Some(key) = argument.path.get_ident() else {
            return Error::new_spanned(
                argument.path,
                "connector_session arguments must be identifiers",
            )
            .to_compile_error()
            .into();
        };
        let target = match key.to_string().as_str() {
            "codec" => &mut codec,
            "io" => &mut io,
            _ => {
                return Error::new_spanned(key, "connector_session supports only `codec` and `io`")
                    .to_compile_error()
                    .into();
            }
        };
        if target.replace(argument.value).is_some() {
            return Error::new_spanned(key, "duplicate connector_session argument")
                .to_compile_error()
                .into();
        }
    }
    let codec = match codec {
        Some(codec) => codec,
        None => {
            return Error::new(Span::call_site(), "missing `codec` argument")
                .to_compile_error()
                .into();
        }
    };
    let io = match io {
        Some(io) => io,
        None => {
            return Error::new(Span::call_site(), "missing `io` argument")
                .to_compile_error()
                .into();
        }
    };
    let mut item = match syn::parse::<syn::ItemImpl>(item) {
        Ok(item) => item,
        Err(error) => return error.to_compile_error().into(),
    };
    if let Err(error) = item.modifiers.require_empty() {
        return error.to_compile_error().into();
    }
    let Some(session) = item
        .trait_
        .as_ref()
        .and_then(|(path, _)| path.segments.last())
    else {
        return Error::new_spanned(
            &item.self_ty,
            "#[connector_session] requires an impl of connector::session::Session",
        )
        .to_compile_error()
        .into();
    };
    if session.ident != "Session" {
        return Error::new_spanned(
            &item.self_ty,
            "#[connector_session] requires an impl of connector::session::Session",
        )
        .to_compile_error()
        .into();
    }
    let (driver, route) = {
        use syn::{Lifetime, PathArguments};

        let mut driver = None;
        let mut route = None;
        if let PathArguments::AngleBracketed(arguments) = &session.arguments {
            for argument in &arguments.args {
                match argument {
                    syn::GenericArgument::Lifetime(lifetime) if driver.is_none() => {
                        driver = Some(lifetime.clone());
                    }
                    value if route.is_none() => route = Some(value.clone()),
                    _ => {}
                }
            }
        }
        (
            driver.unwrap_or_else(|| Lifetime::new("'_", Span::call_site())),
            route.unwrap_or_else(|| syn::parse_quote!(0)),
        )
    };
    for method in ["codec", "activate", "retire_requests", "drain_requests"] {
        use syn::ImplItem;

        if item
            .items
            .iter()
            .any(|member| matches!(member, ImplItem::Fn(function) if function.sig.ident == method))
        {
            return Error::new_spanned(
                &item.self_ty,
                format!("#[connector_session] generates `{method}`; remove the duplicate"),
            )
            .to_compile_error()
            .into();
        }
    }
    if !is_field_path(&codec) {
        return Error::new_spanned(
            codec,
            "`codec` must be a field path such as `protocol.codec`",
        )
        .to_compile_error()
        .into();
    }
    if !is_field_path(&io) {
        return Error::new_spanned(io, "`io` must be a field path such as `port.io`")
            .to_compile_error()
            .into();
    }
    item.items.push(syn::parse_quote! {
        fn codec(&self) -> &Self::Codec {
            &self.#codec
        }
    });
    item.items.push(syn::parse_quote! {
        fn activate(
            &self,
            token: ::dope::manifold::connector::connection::Id<#driver, #route>,
            ready: ::dope::core::driver::schedule::ready::Target<#driver>,
        ) {
            assert!(self.#io.activate(token, ready));
        }
    });
    let generated_retirement: syn::ImplItem = syn::parse_quote! {
        fn retire_requests<'turn>(
            &self,
            token: ::dope::manifold::connector::connection::Id<#driver, #route>,
            work: ::dope::core::driver::schedule::Application<'turn, #driver>,
            region: &mut ::o3::cell::region::Token<#driver>,
        ) -> ::dope::net::link::egress::ClearProgress {
            self.#io.retire(token, work, region)
        }
    };
    item.items.push(syn::parse_quote! {
        fn drain_requests(
            &self,
            token: ::dope::manifold::connector::connection::Id<#driver, #route>,
            parser: &mut <Self::Codec as ::dope::manifold::connector::codec::Codec>::ParseState,
            drain: &mut ::dope::manifold::connector::app::RequestDrain<
                '_,
                #driver,
                Self::Send,
            >,
            region: &mut ::o3::cell::region::Token<#driver>,
        ) -> ::dope::manifold::connector::app::Requests {
            match self.#io.drain_requests(
                region,
                token,
                drain,
                || self.begin(token, parser),
            ) {
                Some(requests) => requests,
                None => ::dope::manifold::connector::app::Requests::default(),
            }
        }
    });
    let mut retirement = Vec::new();
    let mut scheduling = Vec::new();
    item.items.retain(|member| {
        let syn::ImplItem::Fn(function) = member else {
            return true;
        };
        let name = function.sig.ident.to_string();
        if matches!(
            name.as_str(),
            "begin_retirement"
                | "retire_requests"
                | "retire_responses"
                | "defer_close"
                | "is_drained"
                | "disconnect"
        ) {
            retirement.push(member.clone());
            false
        } else if matches!(name.as_str(), "pre_park" | "progress" | "inbound") {
            scheduling.push(member.clone());
            false
        } else {
            true
        }
    });
    retirement.push(generated_retirement);

    let generics = &item.generics;
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let self_ty = &item.self_ty;
    quote! {
        #item

        impl #impl_generics ::dope::manifold::connector::session::Retirement<#driver, #route>
            for #self_ty #where_clause
        {
            #(#retirement)*
        }

        impl #impl_generics ::dope::manifold::connector::session::Scheduling<#driver, #route>
            for #self_ty #where_clause
        {
            #(#scheduling)*
        }
    }
    .into()
}
