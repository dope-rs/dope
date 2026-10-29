mod app;
mod fiber;
mod manifold;
mod work;

pub(crate) use app::{App, Setting};
pub(crate) use fiber::Fiber;
pub(crate) use manifold::Manifold;
use proc_macro2::{Span, TokenStream};
use syn::spanned;
pub(crate) use work::Work;

pub(crate) enum Failure {
    Rule { span: Span, kind: Kind },
    Syntax { source: syn::Error, kind: Kind },
}

pub(crate) enum Kind {
    App(App),
    Fiber(Fiber),
    Manifold(Manifold),
    Work(Work),
}

impl Failure {
    pub(crate) fn new<T>(source: &impl spanned::Spanned, kind: T) -> Self
    where
        Kind: From<T>,
    {
        Self::Rule {
            span: source.span(),
            kind: kind.into(),
        }
    }

    pub(crate) fn at<T>(span: Span, kind: T) -> Self
    where
        Kind: From<T>,
    {
        Self::Rule {
            span,
            kind: kind.into(),
        }
    }

    pub(crate) fn emit(self) -> proc_macro::TokenStream {
        let tokens = match self {
            Self::Rule { span, kind } => kind.tokens(span),
            Self::Syntax { source, kind } => {
                let mut tokens = kind.tokens(source.span());
                tokens.extend(source.into_compile_error());
                tokens
            }
        };
        tokens.into()
    }
}

impl Kind {
    fn tokens(self, span: Span) -> TokenStream {
        let code = match &self {
            Self::App(kind) => kind.code(),
            Self::Fiber(kind) => kind.code(),
            Self::Manifold(kind) => kind.code(),
            Self::Work(kind) => kind.code(),
        };
        quote::quote_spanned!(span=> ::core::compile_error!(#code);)
    }
}
