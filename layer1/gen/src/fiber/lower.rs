use std::mem;

use proc_macro2::TokenStream;
use syn::{
    self, parse, parse_quote, parse_quote_spanned, punctuated, spanned::Spanned, visit_mut,
    visit_mut::VisitMut as _,
};

use crate::{
    Failure,
    failure::{self, Fiber},
    fiber::input::{Expressions, Logging, MatchAssert, Matches, VecInput},
};

pub(crate) struct Lower<'a> {
    pub(super) brand: &'a syn::Ident,
    brand_ty: &'a TokenStream,
    failure: Option<Failure>,
    synchronous: bool,
}

impl<'a> Lower<'a> {
    pub(crate) fn try_lower(
        brand: &'a syn::Ident,
        brand_ty: &'a TokenStream,
        block: &mut syn::Block,
    ) -> Result<(), Failure> {
        let mut lower = Self {
            brand,
            brand_ty,
            failure: None,
            synchronous: false,
        };
        lower.visit_block_mut(block);
        lower.failure.map_or(Ok(()), Err)
    }

    pub(super) fn try_expr(&mut self, node: &mut syn::Expr) -> Result<(), Failure> {
        self.visit_expr_mut(node);
        self.failure.take().map_or(Ok(()), Err)
    }

    pub(super) fn try_attrs(attrs: &[syn::Attribute]) -> Result<(), Failure> {
        attrs.iter().try_for_each(|attr| Self::try_meta(&attr.meta))
    }

    fn try_meta(meta: &syn::Meta) -> Result<(), Failure> {
        if [
            "cfg",
            "doc",
            "allow",
            "expect",
            "warn",
            "deny",
            "forbid",
            "inline",
            "cold",
            "must_use",
            "deprecated",
            "track_caller",
        ]
        .iter()
        .any(|name| meta.path().is_ident(name))
        {
            return Ok(());
        }
        let syn::Meta::List(list) = meta else {
            return Err(Failure::new(meta, Fiber::Attribute));
        };
        if !list.path.is_ident("cfg_attr") {
            return Err(Failure::new(meta, Fiber::Attribute));
        }
        let attrs = list
            .parse_args_with(punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
            .map_err(|source| Failure::Syntax {
                source,
                kind: failure::Kind::Fiber(Fiber::Attribute),
            })?;
        attrs.iter().skip(1).try_for_each(Self::try_meta)
    }

    fn reject(&mut self, node: &impl Spanned) {
        if self.failure.is_none() {
            self.failure = Some(Failure::new(node, Fiber::AsyncScope));
        }
    }

    /// An immediately awaited lexical scope keeps the parent's affine brand.
    /// Attributes cannot replace its audited syntax after this lowering.
    fn scope(node: &mut syn::Expr) -> Option<&mut syn::ExprAsync> {
        match node {
            syn::Expr::Async(scope) if scope.capture.is_none() && scope.attrs.is_empty() => {
                Some(scope)
            }
            syn::Expr::Paren(paren) if paren.attrs.is_empty() => Self::scope(&mut paren.expr),
            syn::Expr::Group(group) if group.attrs.is_empty() => Self::scope(&mut group.expr),
            _ => None,
        }
    }

    /// Checks arguments before logging expands inside a synchronous unit closure.
    fn try_logging(
        &self,
        node: &syn::Macro,
        attrs: &[syn::Attribute],
    ) -> Result<syn::Expr, Failure> {
        Self::try_attrs(attrs)?;
        let Logging(mut entries) = Self::parse(node.tokens.clone(), Fiber::MacroControl)?;
        let mut lower = Self {
            brand: self.brand,
            brand_ty: self.brand_ty,
            failure: None,
            synchronous: true,
        };
        let mut tokens = TokenStream::new();
        for (prefix, value) in &mut entries {
            lower.try_expr(value)?;
            tokens.extend(quote::quote!(#prefix #value,));
        }
        let path = &node.path;
        Ok(parse_quote!((|| -> () { #path!(#tokens); })()))
    }
}

impl visit_mut::VisitMut for Lower<'_> {
    fn visit_attributes_mut(&mut self, attrs: &mut Vec<syn::Attribute>) {
        if self.failure.is_none() {
            self.failure = Self::try_attrs(attrs).err();
        }
    }

    /// Evaluates await operands before borrowing the brand, preserving temporaries.
    /// Immediate lexical scopes retain the same frame; only fiber awaits spend credit.
    fn visit_expr_mut(&mut self, node: &mut syn::Expr) {
        if self.failure.is_some() {
            return;
        }
        if self.synchronous
            && matches!(
                node,
                syn::Expr::Await(_)
                    | syn::Expr::Return(_)
                    | syn::Expr::Break(_)
                    | syn::Expr::Continue(_)
                    | syn::Expr::Try(_)
            )
        {
            self.failure = Some(Failure::new(node, Fiber::MacroControl));
            return;
        }
        match mem::replace(node, syn::Expr::Verbatim(TokenStream::new())) {
            syn::Expr::Macro(value) if Self::is_logging(&value.mac) => {
                match self.try_logging(&value.mac, &value.attrs) {
                    Ok(mut lowered) => {
                        if !value.attrs.is_empty() {
                            let attrs = value.attrs;
                            lowered = parse_quote!({ #(#attrs)* #lowered });
                        }
                        *node = lowered;
                    }
                    Err(failure) => {
                        self.failure = Some(failure);
                        *node = syn::Expr::Macro(value);
                    }
                }
            }
            syn::Expr::Await(mut awaited) => {
                self.visit_attributes_mut(&mut awaited.attrs);
                if awaited.attrs.is_empty()
                    && let Some(scope) = Self::scope(&mut awaited.base)
                {
                    self.visit_block_mut(&mut scope.block);
                    *node = syn::Expr::Await(awaited);
                    return;
                }
                self.visit_expr_mut(&mut awaited.base);
                if self.failure.is_some() {
                    *node = syn::Expr::Await(awaited);
                    return;
                }
                let brand = self.brand;
                let brand_ty = self.brand_ty;
                let span = awaited.await_token.span;
                let base = awaited.base;
                let attrs = awaited.attrs;
                *node = parse_quote_spanned! {span=>
                    #(#attrs)* #brand_ty::awaitable(#base, &mut #brand).await
                };
            }
            taken => {
                *node = taken;
                visit_mut::visit_expr_mut(self, node);
            }
        }
    }

    fn visit_stmt_mut(&mut self, node: &mut syn::Stmt) {
        if self.failure.is_some() {
            return;
        }
        if let syn::Stmt::Macro(value) = node
            && Self::is_logging(&value.mac)
        {
            match self.try_logging(&value.mac, &value.attrs) {
                Ok(lowered) => {
                    let attrs = &value.attrs;
                    *node = parse_quote!(#(#attrs)* #lowered;);
                }
                Err(failure) => self.failure = Some(failure),
            }
        } else {
            visit_mut::visit_stmt_mut(self, node);
        }
    }

    fn visit_expr_async_mut(&mut self, node: &mut syn::ExprAsync) {
        if self.failure.is_none() {
            self.failure = Some(Failure::new(node, Fiber::AsyncScope));
        }
    }

    /// A synchronous closure retains its own compiler-checked await boundary.
    fn visit_expr_closure_mut(&mut self, node: &mut syn::ExprClosure) {
        self.visit_attributes_mut(&mut node.attrs);
        if node.asyncness.is_some() {
            self.reject(node);
        }
    }

    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        self.visit_attributes_mut(&mut node.attrs);
        if node.sig.asyncness.is_some() {
            self.reject(node);
        }
    }

    fn visit_impl_item_fn_mut(&mut self, node: &mut syn::ImplItemFn) {
        self.visit_attributes_mut(&mut node.attrs);
        if node.sig.asyncness.is_some() {
            self.reject(node);
        }
    }

    fn visit_trait_item_fn_mut(&mut self, node: &mut syn::TraitItemFn) {
        self.visit_attributes_mut(&mut node.attrs);
        if node.sig.asyncness.is_some() {
            self.reject(node);
        }
    }

    fn visit_macro_mut(&mut self, node: &mut syn::Macro) {
        if self.failure.is_none() {
            self.failure = self.try_macro(node).err();
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Root {
    Core,
    Std,
    Alloc,
}

#[derive(Clone, Copy)]
enum Kind {
    Matches,
    MatchAssert,
    Vec,
    Expressions,
    Literal,
}

const STD: &[Root] = &[Root::Std];
const CORE: &[Root] = &[Root::Core, Root::Std];
const ALLOC: &[Root] = &[Root::Alloc, Root::Std];
const ALL: &[Root] = &[Root::Core, Root::Std, Root::Alloc];

/// Only listed macros have syntax whose await boundaries the lowering can audit.
const STANDARD: &[(&str, &[Root], Kind)] = &[
    ("matches", CORE, Kind::Matches),
    ("assert_matches", CORE, Kind::MatchAssert),
    ("debug_assert_matches", CORE, Kind::MatchAssert),
    ("vec", ALLOC, Kind::Vec),
    ("format", ALLOC, Kind::Expressions),
    ("print", STD, Kind::Expressions),
    ("println", STD, Kind::Expressions),
    ("eprint", STD, Kind::Expressions),
    ("eprintln", STD, Kind::Expressions),
    ("dbg", STD, Kind::Expressions),
    ("assert", ALL, Kind::Expressions),
    ("assert_eq", ALL, Kind::Expressions),
    ("assert_ne", ALL, Kind::Expressions),
    ("debug_assert", ALL, Kind::Expressions),
    ("debug_assert_eq", ALL, Kind::Expressions),
    ("debug_assert_ne", ALL, Kind::Expressions),
    ("panic", ALL, Kind::Expressions),
    ("unreachable", ALL, Kind::Expressions),
    ("todo", ALL, Kind::Expressions),
    ("unimplemented", ALL, Kind::Expressions),
    ("format_args", ALL, Kind::Expressions),
    ("write", ALL, Kind::Expressions),
    ("writeln", ALL, Kind::Expressions),
    ("cfg", ALL, Kind::Literal),
    ("column", ALL, Kind::Literal),
    ("concat", ALL, Kind::Literal),
    ("env", ALL, Kind::Literal),
    ("file", ALL, Kind::Literal),
    ("line", ALL, Kind::Literal),
    ("module_path", ALL, Kind::Literal),
    ("option_env", ALL, Kind::Literal),
    ("stringify", ALL, Kind::Literal),
];

impl Lower<'_> {
    pub(super) fn try_macro(&mut self, node: &mut syn::Macro) -> Result<(), Failure> {
        let Some(segment) = node.path.segments.last() else {
            return Err(Failure::new(node, Fiber::MacroAwait));
        };
        let name = segment.ident.clone();
        if name == "pin" {
            return Err(Failure::new(node, Fiber::Pin));
        }
        let Some((_, roots, kind)) = STANDARD.iter().find(|(entry, _, _)| name == *entry) else {
            return Err(Failure::new(node, Fiber::MacroAwait));
        };
        let Some(root) = Self::root(node, roots).filter(|root| roots.contains(root)) else {
            return Err(Failure::new(node, Fiber::MacroAwait));
        };
        let tokens = match kind {
            Kind::Literal => node.tokens.clone(),
            Kind::Expressions => {
                Expressions::lower(Self::parse(node.tokens.clone(), Fiber::Expressions)?, self)?
            }
            Kind::Matches => {
                Matches::lower(Self::parse(node.tokens.clone(), Fiber::Matches)?, self)?
            }
            Kind::MatchAssert => {
                MatchAssert::lower(Self::parse(node.tokens.clone(), Fiber::Matches)?, self)?
            }
            Kind::Vec => VecInput::lower(Self::parse(node.tokens.clone(), Fiber::Vec)?, self)?,
        };
        node.tokens = tokens;
        root.apply(&name, &mut node.path);
        Ok(())
    }

    fn parse<T>(tokens: TokenStream, kind: Fiber) -> Result<T, Failure>
    where
        T: parse::Parse,
    {
        match syn::parse2(tokens) {
            Ok(input) => Ok(input),
            Err(source) => Err(Failure::Syntax {
                source,
                kind: failure::Kind::Fiber(kind),
            }),
        }
    }

    /// Bare macros bind to core when available, otherwise to std.
    fn root(node: &syn::Macro, roots: &[Root]) -> Option<Root> {
        if node.path.leading_colon.is_none() && node.path.segments.len() == 1 {
            return Some(if roots.contains(&Root::Core) {
                Root::Core
            } else {
                Root::Std
            });
        }
        if node.path.segments.len() != 2 {
            return None;
        }
        let root = &node.path.segments.first()?.ident;
        if root == "core" {
            Some(Root::Core)
        } else if root == "std" {
            Some(Root::Std)
        } else if root == "alloc" {
            Some(Root::Alloc)
        } else {
            None
        }
    }

    fn is_listed(name: &syn::Ident, names: &[&str]) -> bool {
        names.iter().any(|entry| name == *entry)
    }

    fn is_logging(node: &syn::Macro) -> bool {
        node.path.segments.len() == 2
            && node.path.segments[0].ident == "tracing"
            && Self::is_listed(
                &node.path.segments[1].ident,
                &["trace", "debug", "info", "warn", "error"],
            )
    }
}

impl Root {
    fn apply(self, name: &syn::Ident, path: &mut syn::Path) {
        let root = match self {
            Self::Core => quote::quote!(::core),
            Self::Std => quote::quote!(::std),
            Self::Alloc => quote::quote!(::alloc),
        };
        *path = parse_quote!(#root::#name);
    }
}
