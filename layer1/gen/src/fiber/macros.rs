use std::mem;

use proc_macro2::{Spacing, Span, TokenStream, TokenTree};
use syn::parse;

use crate::{
    Failure,
    failure::{self, Fiber},
    fiber, paths,
};

struct Config {
    driver: syn::Lifetime,
    fiber: paths::Fiber,
}

impl Config {
    fn try_body(
        &self,
        mut body: syn::ExprAsync,
        source: &TokenStream,
    ) -> Result<TokenStream, Failure> {
        if body.capture.is_none() {
            return Err(Failure::new(&body.async_token, Fiber::Move));
        }
        let Self { driver, fiber } = self;
        let abi = fiber.abi();
        let brand_ty = fiber.brand();
        let source = quote::quote!(#source #abi #driver);
        let brand = crate::tokens::ident(&source, "__dope_brand");
        let scope = crate::tokens::ident(&source, "__DopeScope");
        fiber::Lower::try_lower(&brand, brand_ty, &mut body.block)?;
        // The local helper shields generated bindings from unseen outer const names.
        Ok(quote::quote! {
            struct #scope;
            /// # Safety
            /// The pair must remain affine inside one generated state machine.
            unsafe fn #brand<'scope>() -> (
                #brand_ty<'scope, #scope>,
                #abi::future::raw::Seal<'scope, #scope>,
            ) {
                // SAFETY: the caller retains the returned pair in one machine.
                unsafe { #brand_ty::scope() }
            }
            // SAFETY: this pair stays in the single state machine below.
            let #brand = unsafe { #brand::<#driver>() };
            let #brand = (#brand.1, {
                let mut #brand = #brand.0;
                #body
            });
            // SAFETY: the paired body awaits only this brand and retains no waker.
            unsafe { #brand.0.future(#brand.1) }
        })
    }

    fn parse(tokens: TokenStream) -> Result<Self, Failure> {
        let mut driver = TokenStream::new();
        let mut input = tokens.into_iter();
        while let Some(token) = input.next() {
            match token {
                TokenTree::Punct(comma) if comma.as_char() == ',' => {
                    let facade = Self::facade(comma.span(), input.collect())?;
                    return Self::build(driver, Some(facade));
                }
                token => driver.extend([token]),
            }
        }
        Self::build(driver, None)
    }

    fn build(driver: TokenStream, facade: Option<TokenStream>) -> Result<Self, Failure> {
        let driver = syn::parse2(driver).map_err(|source| Failure::Syntax {
            source,
            kind: failure::Kind::Fiber(Fiber::Driver),
        })?;
        let fiber = paths::Fiber::resolve(facade);
        Ok(Self { driver, fiber })
    }

    fn facade(comma: Span, tokens: TokenStream) -> Result<TokenStream, Failure> {
        let mut input = tokens.into_iter();
        let name = match input.next() {
            Some(TokenTree::Ident(name)) => name,
            Some(token) => return Err(Failure::at(token.span(), Fiber::Facade)),
            None => return Err(Failure::at(comma, Fiber::Facade)),
        };
        if name != "crate" {
            return Err(Failure::at(name.span(), Fiber::Facade));
        }
        let equal = match input.next() {
            Some(TokenTree::Punct(equal)) => equal,
            Some(token) => return Err(Failure::at(token.span(), Fiber::Facade)),
            None => return Err(Failure::at(name.span(), Fiber::Facade)),
        };
        if equal.as_char() != '=' {
            return Err(Failure::at(equal.span(), Fiber::Facade));
        }
        let path = syn::parse2::<syn::Path>(input.collect()).map_err(|source| Failure::Syntax {
            source,
            kind: failure::Kind::Fiber(Fiber::Facade),
        })?;
        Ok(quote::quote!(#path))
    }
}

pub(crate) struct Macro(Result<(Config, syn::ExprAsync), Failure>);

impl parse::Parse for Macro {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let tokens = input.parse()?;
        Ok(Self(Self::from_tokens(tokens)))
    }
}

pub(crate) struct Args(Result<Config, Failure>);

impl parse::Parse for Args {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let tokens = input.parse()?;
        Ok(Self(Config::parse(tokens)))
    }
}

impl Macro {
    fn from_tokens(tokens: TokenStream) -> Result<(Config, syn::ExprAsync), Failure> {
        let mut config = TokenStream::new();
        let mut end = Span::call_site();
        let mut input = tokens.into_iter();
        while let Some(token) = input.next() {
            end = token.span();
            match token {
                TokenTree::Punct(equal)
                    if equal.as_char() == '=' && equal.spacing() == Spacing::Joint =>
                {
                    match input.next() {
                        Some(TokenTree::Punct(greater)) if greater.as_char() == '>' => {
                            let config = Config::parse(config)?;
                            let body =
                                syn::parse2(input.collect()).map_err(|source| Failure::Syntax {
                                    source,
                                    kind: failure::Kind::Fiber(Fiber::Body),
                                })?;
                            return Ok((config, body));
                        }
                        Some(token) => {
                            end = token.span();
                            config.extend([TokenTree::Punct(equal), token]);
                        }
                        None => return Err(Failure::at(end, Fiber::Arrow)),
                    }
                }
                token => config.extend([token]),
            }
        }
        Err(Failure::at(end, Fiber::Arrow))
    }

    pub(crate) fn try_expand(self) -> Result<proc_macro::TokenStream, Failure> {
        let (config, body) = self.0?;
        let source = quote::quote!(#body);
        let body = config.try_body(body, &source)?;
        Ok(quote::quote!({ #body }).into())
    }
}

pub(crate) struct Attribute {
    config: Result<Config, Failure>,
    item: syn::ItemFn,
}

impl Attribute {
    pub(crate) fn new(args: Args, item: syn::ItemFn) -> Self {
        Self {
            config: args.0,
            item,
        }
    }

    /// Moves complete arguments into the fiber before applying their patterns.
    pub(crate) fn try_expand(self) -> Result<proc_macro::TokenStream, Failure> {
        use syn::{ReturnType, parse_quote};

        let Self { config, item } = self;
        let config = config?;
        let source = quote::quote!(#item);
        let syn::ItemFn {
            attrs,
            vis,
            modifiers: mods,
            mut sig,
            mut block,
        } = item;
        fiber::Lower::try_attrs(&attrs)?;
        if let Some(modifier) = &mods.defaultness {
            return Err(Failure::new(modifier, Fiber::Modifier));
        }
        let async_kw = match sig.asyncness.take() {
            Some(mark) => mark,
            None => return Err(Failure::new(&sig.fn_token, Fiber::AsyncFn)),
        };
        for argument in sig.inputs.iter_mut().rev() {
            match argument {
                syn::FnArg::Receiver(_) => {
                    block.stmts.insert(0, parse_quote!(let _ = &self;));
                }
                syn::FnArg::Typed(argument) => {
                    let name = Self::capture(&argument.pat)
                        .ok_or_else(|| Failure::new(&argument.pat, Fiber::ArgumentCapture))?
                        .clone();
                    let pattern =
                        mem::replace(&mut argument.pat, Box::new(parse_quote!(#name @ _)));
                    *block = parse_quote!({
                        match { #name } {
                            #pattern => #block,
                        }
                    });
                }
            }
        }
        let body = config.try_body(
            syn::ExprAsync {
                attrs: Vec::new(),
                async_token: async_kw,
                capture: Some(Default::default()),
                modifiers: Default::default(),
                block: *block,
            },
            &source,
        )?;
        let (arrow, output) = match mem::replace(&mut sig.output, ReturnType::Default) {
            ReturnType::Default => (Default::default(), parse_quote! { () }),
            ReturnType::Type(arrow, ty) => (arrow, *ty),
        };
        let Config { driver, fiber } = config;
        let abi = fiber.abi();
        sig.output = ReturnType::Type(
            arrow,
            Box::new(parse_quote! { impl #abi::Fiber<#driver, Output = #output> }),
        );
        let outer = attrs
            .iter()
            .filter(|attr| matches!(attr.style, syn::AttrStyle::Outer));
        let inner = attrs
            .iter()
            .filter(|attr| matches!(attr.style, syn::AttrStyle::Inner(_)));
        Ok(quote::quote! {
            #(#outer)* #vis #sig { #(#inner)* #body }
        }
        .into())
    }

    fn capture(pattern: &syn::Pat) -> Option<&syn::Ident> {
        match pattern {
            syn::Pat::Ident(binding) => Some(&binding.ident),
            syn::Pat::Or(pattern) => pattern.cases.iter().find_map(Self::capture),
            syn::Pat::Paren(pattern) => Self::capture(&pattern.pat),
            syn::Pat::Reference(pattern) => Self::capture(&pattern.pat),
            syn::Pat::Slice(pattern) => pattern.elems.iter().find_map(Self::capture),
            syn::Pat::Struct(pattern) => pattern
                .fields
                .iter()
                .find_map(|field| Self::capture(&field.pat)),
            syn::Pat::Tuple(pattern) => pattern.elems.iter().find_map(Self::capture),
            syn::Pat::TupleStruct(pattern) => pattern.elems.iter().find_map(Self::capture),
            syn::Pat::Type(pattern) => Self::capture(&pattern.pat),
            _ => None,
        }
    }
}
