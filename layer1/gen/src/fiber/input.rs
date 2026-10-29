use proc_macro2::TokenStream;
use syn::{self, parse, punctuated};

use crate::{Failure, fiber::Lower};

pub(super) struct Expressions(pub(super) punctuated::Punctuated<syn::Expr, syn::Token![,]>);

pub(super) struct Matches {
    pub(super) expr: syn::Expr,
    pub(super) comma: syn::Token![,],
    pub(super) pat: syn::Pat,
    pub(super) guard: Option<(syn::Token![if], syn::Expr)>,
}

pub(super) struct MatchAssert {
    matches: Matches,
    message: Option<(syn::Token![,], Expressions)>,
}

pub(super) struct Logging(pub(super) Vec<(TokenStream, syn::Expr)>);

pub(super) enum VecInput {
    List(Expressions),
    Repeat {
        value: syn::Expr,
        semi: syn::Token![;],
        len: Box<syn::Expr>,
    },
}

impl parse::Parse for Expressions {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(punctuated::Punctuated::parse_terminated(input)?))
    }
}

impl parse::Parse for Matches {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::Pat;
        let matches = Self {
            expr: input.parse()?,
            comma: input.parse()?,
            pat: Pat::parse_multi_with_leading_vert(input)?,
            guard: if input.peek(syn::Token![if]) {
                Some((input.parse()?, input.parse()?))
            } else {
                None
            },
        };
        if input.peek(syn::Token![,]) {
            let trailing = input.fork();
            trailing.parse::<syn::Token![,]>()?;
            if trailing.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(matches)
    }
}

impl parse::Parse for VecInput {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self::List(Expressions(punctuated::Punctuated::new())));
        }
        let first = input.parse()?;
        if input.peek(syn::Token![;]) {
            return Ok(Self::Repeat {
                value: first,
                semi: input.parse()?,
                len: Box::new(input.parse()?),
            });
        }
        let mut values = punctuated::Punctuated::new();
        values.push_value(first);
        while !input.is_empty() {
            values.push_punct(input.parse()?);
            if input.is_empty() {
                break;
            }
            values.push_value(input.parse()?);
        }
        Ok(Self::List(Expressions(values)))
    }
}

impl parse::Parse for MatchAssert {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self {
            matches: input.parse()?,
            message: if input.is_empty() {
                None
            } else {
                Some((input.parse()?, input.parse()?))
            },
        })
    }
}

impl parse::Parse for Logging {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::ext::IdentExt as _;

        let mut values = Vec::new();
        while !input.is_empty() {
            let mut prefix = TokenStream::new();
            let key = input.fork();
            if key.call(syn::Ident::parse_any).is_ok() {
                let mut count = 1;
                while key.peek(syn::Token![.]) {
                    key.parse::<syn::Token![.]>()?;
                    if key.call(syn::Ident::parse_any).is_err() {
                        break;
                    }
                    count += 2;
                }
                if key.peek(syn::Token![:]) && !key.peek(syn::Token![::])
                    || key.peek(syn::Token![=]) && !key.peek(syn::Token![==])
                {
                    for _ in 0..=count {
                        prefix.extend([input.parse::<proc_macro2::TokenTree>()?]);
                    }
                }
            }
            if input.peek(syn::Token![?]) || input.peek(syn::Token![%]) {
                prefix.extend([input.parse::<proc_macro2::TokenTree>()?]);
            }
            values.push((prefix, input.parse()?));
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(Self(values))
    }
}

impl Expressions {
    pub(super) fn lower(mut input: Self, lower: &mut Lower<'_>) -> Result<TokenStream, Failure> {
        for expr in &mut input.0 {
            lower.try_expr(expr)?;
        }
        let args = input.0;
        Ok(quote::quote! { #args })
    }
}

impl Matches {
    pub(super) fn lower(mut input: Self, lower: &mut Lower<'_>) -> Result<TokenStream, Failure> {
        lower.try_expr(&mut input.expr)?;
        if let Some((_, guard)) = &mut input.guard {
            lower.try_expr(guard)?;
        }
        let Self {
            expr,
            comma,
            pat,
            guard,
        } = input;
        Ok(match guard {
            Some((if_token, guard)) => quote::quote! { #expr #comma #pat #if_token #guard },
            None => quote::quote! { #expr #comma #pat },
        })
    }
}

impl MatchAssert {
    pub(super) fn lower(input: Self, lower: &mut Lower<'_>) -> Result<TokenStream, Failure> {
        let matched = Matches::lower(input.matches, lower)?;
        Ok(match input.message {
            Some((comma, message)) => {
                let message = Expressions::lower(message, lower)?;
                quote::quote!(#matched #comma #message)
            }
            None => matched,
        })
    }
}

impl VecInput {
    pub(super) fn lower(input: Self, lower: &mut Lower<'_>) -> Result<TokenStream, Failure> {
        Ok(match input {
            Self::List(args) => return Expressions::lower(args, lower),
            Self::Repeat {
                mut value,
                semi,
                mut len,
            } => {
                lower.try_expr(&mut value)?;
                lower.try_expr(&mut len)?;
                quote::quote! { #value #semi #len }
            }
        })
    }
}
