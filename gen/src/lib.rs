#![warn(unreachable_pub)]

extern crate proc_macro;

mod derive;
mod dispatcher;
mod fiber;
mod forward;

use dispatcher::DispatcherSpec;
use fiber::FiberFn;
use forward::Forward;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    DeriveInput, Error, Expr, GenericArgument, ImplItem, ItemFn, ItemImpl, Lifetime, Member,
    MetaNameValue, PathArguments, Token, parse_macro_input,
};

fn is_field_path(expression: &Expr) -> bool {
    match expression {
        Expr::Path(path) => {
            path.qself.is_none()
                && path.path.leading_colon.is_none()
                && path.path.segments.len() == 1
                && matches!(path.path.segments[0].arguments, PathArguments::None)
        }
        Expr::Field(field) => {
            matches!(field.member, Member::Named(_)) && is_field_path(&field.base)
        }
        _ => false,
    }
}

#[proc_macro_attribute]
pub fn fiber_fn(attr: TokenStream, input: TokenStream) -> TokenStream {
    let driver = parse_macro_input!(attr as Lifetime);
    let item = parse_macro_input!(input as ItemFn);
    FiberFn::new(driver, item).expand()
}

#[proc_macro]
pub fn fiber(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as fiber::Input);
    input.expand()
}

#[proc_macro_derive(Forward, attributes(forward))]
pub fn forward(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    Forward::new(input).expand()
}

#[proc_macro_derive(Dispatcher, attributes(manifold, coordinate))]
pub fn dispatcher(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    DispatcherSpec::derive(input)
}

#[proc_macro_attribute]
pub fn connector_session(attr: TokenStream, item: TokenStream) -> TokenStream {
    let arguments = match Punctuated::<MetaNameValue, Token![,]>::parse_terminated.parse(attr) {
        Ok(arguments) => arguments,
        Err(error) => return error.to_compile_error().into(),
    };
    let mut codec: Option<Expr> = None;
    let mut io: Option<Expr> = None;
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
    let mut item = match syn::parse::<ItemImpl>(item) {
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
    let driver = match &session.arguments {
        PathArguments::AngleBracketed(arguments) => arguments.args.iter().find_map(|argument| {
            if let GenericArgument::Lifetime(lifetime) = argument {
                Some(lifetime.clone())
            } else {
                None
            }
        }),
        PathArguments::None => None,
        PathArguments::Parenthesized(_) => None,
    }
    .unwrap_or_else(|| Lifetime::new("'_", Span::call_site()));
    for method in ["codec", "activate", "drain_requests"] {
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
            token: ::dope::driver::token::Token,
            ready: ::dope::driver::ready::ReadyKey<#driver>,
            _region: &mut ::o3::cell::RegionToken<#driver>,
        ) {
            assert!(self.#io.activate(token, ready));
        }
    });
    item.items.push(syn::parse_quote! {
        fn drain_requests(
            &self,
            token: ::dope::driver::token::Token,
            push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
            _region: &mut ::o3::cell::RegionToken<#driver>,
        ) -> ::dope::manifold::connector::app::Requests {
            match self.#io.drain_requests(token, push) {
                Some(requests) => requests,
                None => ::dope::manifold::connector::app::Requests::default(),
            }
        }
    });
    quote!(#item).into()
}
