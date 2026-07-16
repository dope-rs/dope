#![warn(unreachable_pub)]

extern crate proc_macro;

mod derive;
mod dispatcher;
mod fiber;
mod forward;

use dispatcher::DispatcherSpec;
use fiber::Fiber;
use forward::Forward;
use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    DeriveInput, Error, Expr, GenericArgument, GenericParam, ImplItem, ItemFn, ItemImpl, Lifetime,
    LifetimeParam, Member, MetaNameValue, PathArguments, Token, parse_macro_input,
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
    Fiber::attribute(attr, input)
}

#[proc_macro]
pub fn fiber(input: TokenStream) -> TokenStream {
    Fiber::expression(input)
}

#[proc_macro_derive(Forward, attributes(forward))]
pub fn forward(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    Forward::derive(input)
}

#[proc_macro_derive(Dispatcher, attributes(manifold, coordinate))]
pub fn dispatcher(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    DispatcherSpec::derive(input)
}

#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return Error::new(Span::call_site(), "#[handler] takes no arguments")
            .to_compile_error()
            .into();
    }

    let mut item_fn = parse_macro_input!(item as ItemFn);

    if item_fn.sig.asyncness.is_none() {
        return Error::new_spanned(&item_fn.sig, "#[handler] requires `async fn`")
            .to_compile_error()
            .into();
    }
    let driver: syn::Lifetime = syn::parse_quote!('__dope_handler);
    item_fn.sig.generics.params.insert(
        0,
        GenericParam::Lifetime(LifetimeParam::new(driver.clone())),
    );
    Fiber::attribute(quote!(#driver).into(), quote!(#item_fn).into())
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
    let Some(session) = item
        .trait_
        .as_ref()
        .and_then(|(_, path, _)| path.segments.last())
    else {
        return Error::new_spanned(
            &item.self_ty,
            "#[connector_session] requires an impl of connector::Session",
        )
        .to_compile_error()
        .into();
    };
    if session.ident != "Session" {
        return Error::new_spanned(
            &item.self_ty,
            "#[connector_session] requires an impl of connector::Session",
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
    let dope = match crate_name("dope") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let name = Ident::new(&name, Span::call_site());
            quote!(::#name)
        }
        Err(error) => {
            return Error::new(
                Span::call_site(),
                format!("connector_session could not resolve the `dope` crate: {error}"),
            )
            .to_compile_error()
            .into();
        }
    };
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
            token: #dope::driver::token::Token,
            ready: #dope::driver::ready::ReadyKey<#driver>,
        ) {
            assert!(self.#io.activate(token, ready));
        }
    });
    item.items.push(syn::parse_quote! {
        fn drain_requests(
            &self,
            token: #dope::driver::token::Token,
            push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
        ) -> #dope::manifold::connector::Requests {
            match self.#io.drain_requests(token, push) {
                Some(requests) => requests,
                None => #dope::manifold::connector::Requests::default(),
            }
        }
    });
    quote!(#item).into()
}
