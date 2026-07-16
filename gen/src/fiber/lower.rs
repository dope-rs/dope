use proc_macro2::TokenTree;
use quote::quote;
use std::mem;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit_mut::{self, VisitMut};
use syn::{
    Error, Expr, ExprAsync, ExprAwait, ExprClosure, Ident, ImplItemFn, ItemFn, Macro, Pat, Token,
    TraitItemFn,
};

struct Matches {
    expr: Expr,
    comma: Token![,],
    pat: Pat,
    guard: Option<Expr>,
}

impl Parse for Matches {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self {
            expr: input.parse()?,
            comma: input.parse()?,
            pat: Pat::parse_multi_with_leading_vert(input)?,
            guard: if input.peek(Token![if]) {
                input.parse::<Token![if]>()?;
                Some(input.parse()?)
            } else {
                None
            },
        })
    }
}

enum VecInput {
    List(Punctuated<Expr, Token![,]>),
    Repeat(Expr, Token![;], Box<Expr>),
}

impl Parse for VecInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let first = input.parse()?;
        if input.peek(Token![;]) {
            return Ok(Self::Repeat(
                first,
                input.parse()?,
                Box::new(input.parse()?),
            ));
        }
        let mut expressions = Punctuated::new();
        expressions.push_value(first);
        while input.peek(Token![,]) {
            expressions.push_punct(input.parse()?);
            if input.is_empty() {
                break;
            }
            expressions.push_value(input.parse()?);
        }
        Ok(Self::List(expressions))
    }
}

pub(super) struct Lowerer<'a> {
    brand: &'a Ident,
    errors: Vec<Error>,
}

impl<'a> Lowerer<'a> {
    pub(super) fn lower(brand: &'a Ident, block: &mut syn::Block) -> Result<(), Vec<Error>> {
        let mut lowerer = Self::new(brand);
        lowerer.visit_block_mut(block);
        lowerer.finish()
    }

    fn new(brand: &'a Ident) -> Self {
        Self {
            brand,
            errors: Vec::new(),
        }
    }

    fn finish(self) -> Result<(), Vec<Error>> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }

    fn contains_await(tokens: proc_macro2::TokenStream) -> bool {
        tokens.into_iter().any(|token| match token {
            TokenTree::Group(group) => Self::contains_await(group.stream()),
            TokenTree::Ident(ident) => ident == "await",
            _ => false,
        })
    }

    fn path(node: &Macro) -> Option<(&'static str, String)> {
        if node.path.leading_colon.is_none() || node.path.segments.len() < 2 {
            return None;
        }
        let root = node.path.segments.first()?.ident.to_string();
        let name = node.path.segments.last()?.ident.to_string();
        match root.as_str() {
            "core" => Some(("core", name)),
            "std" => Some(("std", name)),
            "alloc" => Some(("alloc", name)),
            _ => None,
        }
    }

    fn expressions(&mut self, node: &mut Macro) -> syn::Result<()> {
        let parser = Punctuated::<Expr, Token![,]>::parse_terminated;
        let mut expressions = parser.parse2(node.tokens.clone())?;
        for expression in &mut expressions {
            self.visit_expr_mut(expression);
        }
        node.tokens = quote! { #expressions };
        Ok(())
    }

    fn matches(&mut self, node: &mut Macro) -> syn::Result<()> {
        let mut input = syn::parse2::<Matches>(node.tokens.clone())?;
        self.visit_expr_mut(&mut input.expr);
        if let Some(guard) = &mut input.guard {
            self.visit_expr_mut(guard);
        }
        let Matches {
            expr,
            comma,
            pat,
            guard,
        } = input;
        node.tokens = match guard {
            Some(guard) => quote! { #expr #comma #pat if #guard },
            None => quote! { #expr #comma #pat },
        };
        Ok(())
    }

    fn vec(&mut self, node: &mut Macro) -> syn::Result<()> {
        if node.tokens.is_empty() {
            return Ok(());
        }
        match syn::parse2::<VecInput>(node.tokens.clone())? {
            VecInput::List(mut expressions) => {
                for expression in &mut expressions {
                    self.visit_expr_mut(expression);
                }
                node.tokens = quote! { #expressions };
            }
            VecInput::Repeat(mut value, semi, mut len) => {
                self.visit_expr_mut(&mut value);
                self.visit_expr_mut(&mut len);
                node.tokens = quote! { #value #semi #len };
            }
        }
        Ok(())
    }

    fn reject_opaque(&mut self, node: &Macro) {
        self.errors.push(Error::new_spanned(
            node,
            "cannot lower `.await` inside this macro; hoist the await into a `let` \
             binding before the macro, or use a supported ::std/::core macro",
        ));
    }

    fn reject_async(&mut self, node: &impl quote::ToTokens) {
        self.errors.push(Error::new_spanned(
            node,
            "independent async scope is not a fiber",
        ));
    }
}

impl VisitMut for Lowerer<'_> {
    fn visit_expr_await_mut(&mut self, node: &mut ExprAwait) {
        self.visit_expr_mut(&mut node.base);
        let brand = self.brand;
        let span = node.base.span();
        let base = mem::replace(&mut *node.base, syn::parse_quote! { () });
        *node.base = syn::parse_quote_spanned! {span=>
            ::dope_fiber::__private::Brand::awaitable(&#brand, #base)
        };
    }

    fn visit_expr_async_mut(&mut self, node: &mut ExprAsync) {
        self.reject_async(node);
    }

    fn visit_expr_closure_mut(&mut self, node: &mut ExprClosure) {
        if node.asyncness.is_some() {
            self.reject_async(node);
        } else {
            visit_mut::visit_expr_closure_mut(self, node);
        }
    }

    fn visit_item_fn_mut(&mut self, node: &mut ItemFn) {
        if node.sig.asyncness.is_some() {
            self.reject_async(node);
        } else {
            visit_mut::visit_item_fn_mut(self, node);
        }
    }

    fn visit_impl_item_fn_mut(&mut self, node: &mut ImplItemFn) {
        if node.sig.asyncness.is_some() {
            self.reject_async(node);
        } else {
            visit_mut::visit_impl_item_fn_mut(self, node);
        }
    }

    fn visit_trait_item_fn_mut(&mut self, node: &mut TraitItemFn) {
        if node.sig.asyncness.is_some() {
            self.reject_async(node);
        } else {
            visit_mut::visit_trait_item_fn_mut(self, node);
        }
    }

    fn visit_macro_mut(&mut self, node: &mut Macro) {
        let path = Self::path(node);
        let is_pin = match &path {
            Some((_, name)) => name == "pin",
            None => node.path.segments.len() == 1 && node.path.segments[0].ident == "pin",
        };
        if is_pin {
            self.errors.push(Error::new_spanned(
                node,
                "pin! is not supported in a fiber; store the owned Fiber directly",
            ));
            return;
        }
        if !Self::contains_await(node.tokens.clone()) {
            return;
        }
        let Some((root, name)) = path else {
            if node.path.leading_colon.is_none() && node.path.segments.len() == 1 {
                let name = node.path.segments[0].ident.to_string();
                let result = match name.as_str() {
                    "vec" => {
                        node.path = syn::parse_quote!(::std::vec);
                        self.vec(node)
                    }
                    "format" => {
                        node.path = syn::parse_quote!(::std::format);
                        self.expressions(node)
                    }
                    "matches" => {
                        node.path = syn::parse_quote!(::core::matches);
                        self.matches(node)
                    }
                    _ => {
                        self.reject_opaque(node);
                        return;
                    }
                };
                if let Err(error) = result {
                    self.errors.push(error);
                }
                return;
            }
            self.reject_opaque(node);
            return;
        };
        let result = match (root, name.as_str()) {
            ("core" | "std", "matches") => self.matches(node),
            ("alloc", "vec") | ("std", "vec") => self.vec(node),
            (_, "assert" | "assert_eq" | "assert_ne" | "debug_assert")
            | (_, "debug_assert_eq" | "debug_assert_ne" | "panic" | "unreachable")
            | (_, "todo" | "unimplemented" | "format_args" | "write" | "writeln")
            | ("alloc", "format")
            | ("std", "format" | "print" | "println" | "eprint" | "eprintln" | "dbg") => {
                self.expressions(node)
            }
            (_, "cfg" | "column" | "concat" | "env" | "file" | "line")
            | (_, "module_path" | "option_env" | "stringify") => Ok(()),
            _ => {
                self.reject_opaque(node);
                Ok(())
            }
        };
        if let Err(error) = result {
            self.errors.push(error);
        }
    }
}
