mod lower;

use lower::Lowerer;
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use std::mem;
use syn::parse::{Parse, ParseStream};
use syn::{Error, Expr, ExprAsync, ItemFn, Lifetime, ReturnType, Token, parse_macro_input};

struct Input {
    driver: Lifetime,
    expr: Expr,
}

impl Parse for Input {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let driver = input.parse()?;
        input.parse::<Token![=>]>()?;
        let expr = input.parse()?;
        Ok(Self { driver, expr })
    }
}

pub(crate) struct Fiber;

impl Fiber {
    fn compile_error_tokens(errors: impl IntoIterator<Item = Error>) -> proc_macro2::TokenStream {
        errors
            .into_iter()
            .map(|error| error.to_compile_error())
            .collect::<proc_macro2::TokenStream>()
    }

    fn compile_errors(errors: impl IntoIterator<Item = Error>) -> TokenStream {
        Self::compile_error_tokens(errors).into()
    }

    pub(crate) fn attribute(attr: TokenStream, input: TokenStream) -> TokenStream {
        let driver = parse_macro_input!(attr as Lifetime);
        let mut item = parse_macro_input!(input as ItemFn);
        if item.sig.asyncness.take().is_none() {
            return Error::new_spanned(item.sig.fn_token, "fiber requires async fn")
                .to_compile_error()
                .into();
        }
        if !item
            .sig
            .generics
            .lifetimes()
            .any(|lifetime| lifetime.lifetime.ident == driver.ident)
        {
            return Error::new_spanned(&item.sig.generics, "fiber lifetime is not declared")
                .to_compile_error()
                .into();
        }

        let output = match &item.sig.output {
            ReturnType::Default => syn::parse_quote! { () },
            ReturnType::Type(_, ty) => (**ty).clone(),
        };
        let brand = Ident::new("__dope_brand", Span::mixed_site());
        if let Err(errors) = Lowerer::lower(&brand, &mut item.block) {
            return Self::compile_errors(errors);
        }
        let block = mem::replace(&mut *item.block, syn::parse_quote! {{}});
        item.sig.output = syn::parse_quote! {
            -> impl ::dope_fiber::Fiber<#driver, Output = #output>
        };
        *item.block = syn::parse_quote! {{
            let (#brand, __dope_seal) = unsafe {
                ::dope_fiber::__private::Brand::<#driver>::scope()
            };
            let __dope_future = async move #block;
            ::dope_fiber::__private::Seal::future(__dope_seal, __dope_future)
        }};
        quote!(#item).into()
    }

    fn async_expression(expression: Expr) -> Result<ExprAsync, Box<Expr>> {
        match expression {
            Expr::Async(expression) => Ok(expression),
            Expr::Group(mut group) => match Self::async_expression(*group.expr) {
                Ok(expression) => Ok(expression),
                Err(expression) => {
                    group.expr = expression;
                    Err(Box::new(Expr::Group(group)))
                }
            },
            Expr::Paren(mut paren) => match Self::async_expression(*paren.expr) {
                Ok(expression) => Ok(expression),
                Err(expression) => {
                    paren.expr = expression;
                    Err(Box::new(Expr::Paren(paren)))
                }
            },
            expression => Err(Box::new(expression)),
        }
    }

    pub(crate) fn expression(input: TokenStream) -> TokenStream {
        let Input { driver, expr } = parse_macro_input!(input as Input);
        let mut expression = match Self::async_expression(expr) {
            Ok(expression) => expression,
            Err(expr) => {
                return quote! {
                    ::dope_fiber::IntoFiber::<#driver>::into_fiber(#expr)
                }
                .into();
            }
        };
        if expression.capture.is_none() {
            return Error::new_spanned(expression.async_token, "fiber async block must be move")
                .to_compile_error()
                .into();
        }
        let brand = Ident::new("__dope_brand", Span::mixed_site());
        if let Err(errors) = Lowerer::lower(&brand, &mut expression.block) {
            let errors = Self::compile_error_tokens(errors);
            return quote! {{ #errors }}.into();
        }
        quote! {
            {
                let (#brand, __dope_seal) = unsafe {
                    ::dope_fiber::__private::Brand::<#driver>::scope()
                };
                let __dope_future = #expression;
                ::dope_fiber::__private::Seal::future(__dope_seal, __dope_future)
            }
        }
        .into()
    }
}
