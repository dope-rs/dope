use syn::parse;

use crate::lower;

type ParseError = syn::Error;
type MacroIdent = proc_macro2::Ident;
type MacroSpan = proc_macro2::Span;

struct Config {
    driver: syn::Lifetime,
    fiber: syn::Path,
}

impl Config {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::Token;

        let driver = input.parse()?;
        input.parse::<Token![,]>()?;
        input.parse::<Token![crate]>()?;
        input.parse::<Token![=]>()?;
        let fiber = input.parse()?;
        Ok(Self { driver, fiber })
    }
}

pub(crate) struct Macro {
    config: Config,
    expr: syn::Expr,
}

impl parse::Parse for Macro {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::Token;
        let config = Config::parse(input)?;
        input.parse::<Token![=>]>()?;
        let expr = input.parse()?;
        Ok(Self { config, expr })
    }
}

pub(crate) struct AttributeArguments(Config);

impl parse::Parse for AttributeArguments {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::Token;

        let config = Config::parse(input)?;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
        Ok(Self(config))
    }
}

fn async_expression(expression: syn::Expr) -> Result<syn::ExprAsync, Box<syn::Expr>> {
    use syn::Expr;
    match expression {
        Expr::Async(expression) => Ok(expression),
        Expr::Group(mut group) => match async_expression(*group.expr) {
            Ok(expression) => Ok(expression),
            Err(expression) => {
                group.expr = expression;
                Err(Box::new(Expr::Group(group)))
            }
        },
        Expr::Paren(mut paren) => match async_expression(*paren.expr) {
            Ok(expression) => Ok(expression),
            Err(expression) => {
                paren.expr = expression;
                Err(Box::new(Expr::Paren(paren)))
            }
        },
        expression => Err(Box::new(expression)),
    }
}

impl Macro {
    pub(crate) fn expand(self) -> proc_macro::TokenStream {
        let Self {
            config: Config { driver, fiber },
            expr,
        } = self;
        let mut expression = match async_expression(expr) {
            Ok(expression) => expression,
            Err(expr) => {
                return quote::quote! {
                    #fiber::abi::IntoFiber::<#driver>::into_fiber(#expr)
                }
                .into();
            }
        };
        if expression.capture.is_none() {
            return ParseError::new_spanned(
                expression.async_token,
                "fiber async block must be move",
            )
            .to_compile_error()
            .into();
        }
        let brand = MacroIdent::new("__dope_brand", MacroSpan::mixed_site());
        let seal = MacroIdent::new("__dope_seal", MacroSpan::mixed_site());
        let future = MacroIdent::new("__dope_future", MacroSpan::mixed_site());
        if let Err(errors) = lower::Lower::lower(&brand, &mut expression.block) {
            let errors = lower::Lower::errors(errors);
            return quote::quote! {{ #errors }}.into();
        }
        quote::quote! {
            {
                let (#brand, #seal) = unsafe {
                    #fiber::abi::future::raw::Brand::<#driver>::scope()
                };
                let #future = #expression;
                #seal.future(#future)
            }
        }
        .into()
    }
}

pub(crate) struct Attribute {
    config: Config,
    item: syn::ItemFn,
}

impl Attribute {
    pub(crate) fn new(arguments: AttributeArguments, item: syn::ItemFn) -> Self {
        Self {
            config: arguments.0,
            item,
        }
    }

    pub(crate) fn expand(self) -> proc_macro::TokenStream {
        use std::mem::replace;

        use syn::{ReturnType, parse_quote};

        let Self {
            config: Config { driver, fiber },
            mut item,
        } = self;
        if let Err(error) = item.modifiers.require_empty() {
            return error.to_compile_error().into();
        }
        if item.sig.asyncness.take().is_none() {
            return ParseError::new_spanned(item.sig.fn_token, "fiber requires async fn")
                .to_compile_error()
                .into();
        }
        if !item
            .sig
            .generics
            .lifetimes()
            .any(|lifetime| lifetime.lifetime.ident == driver.ident)
        {
            return ParseError::new_spanned(&item.sig.generics, "fiber lifetime is not declared")
                .to_compile_error()
                .into();
        }

        let output = match &item.sig.output {
            ReturnType::Default => parse_quote! { () },
            ReturnType::Type(_, ty) => (**ty).clone(),
        };
        let brand = MacroIdent::new("__dope_brand", MacroSpan::mixed_site());
        let seal = MacroIdent::new("__dope_seal", MacroSpan::mixed_site());
        let future = MacroIdent::new("__dope_future", MacroSpan::mixed_site());
        if let Err(errors) = lower::Lower::lower(&brand, &mut item.block) {
            return lower::Lower::errors(errors).into();
        }
        let block = replace(&mut *item.block, parse_quote! {{}});
        item.sig.output = parse_quote! {
            -> impl #fiber::abi::Fiber<#driver, Output = #output>
        };
        *item.block = parse_quote! {{
            let (#brand, #seal) = unsafe {
                #fiber::abi::future::raw::Brand::<#driver>::scope()
            };
            let #future = async move #block;
            #seal.future(#future)
        }};
        quote::quote!(#item).into()
    }
}
