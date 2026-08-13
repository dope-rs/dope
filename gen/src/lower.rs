use syn::{
    self,
    parse::{self, Parser as _},
    punctuated,
    spanned::Spanned as _,
    visit_mut::{self, VisitMut as _},
};

struct Matches {
    expr: syn::Expr,
    comma: syn::Token![,],
    pat: syn::Pat,
    guard: Option<syn::Expr>,
}

impl parse::Parse for Matches {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        use syn::Pat;
        Ok(Self {
            expr: input.parse()?,
            comma: input.parse()?,
            pat: Pat::parse_multi_with_leading_vert(input)?,
            guard: if input.peek(syn::Token![if]) {
                input.parse::<syn::Token![if]>()?;
                Some(input.parse()?)
            } else {
                None
            },
        })
    }
}

enum VecInput {
    List(punctuated::Punctuated<syn::Expr, syn::Token![,]>),
    Repeat(syn::Expr, syn::Token![;], Box<syn::Expr>),
}

impl parse::Parse for VecInput {
    fn parse(input: parse::ParseStream<'_>) -> syn::Result<Self> {
        let first = input.parse()?;
        if input.peek(syn::Token![;]) {
            return Ok(Self::Repeat(
                first,
                input.parse()?,
                Box::new(input.parse()?),
            ));
        }
        let mut expressions = punctuated::Punctuated::new();
        expressions.push_value(first);
        while input.peek(syn::Token![,]) {
            expressions.push_punct(input.parse()?);
            if input.is_empty() {
                break;
            }
            expressions.push_value(input.parse()?);
        }
        Ok(Self::List(expressions))
    }
}

pub(crate) struct Lower<'a> {
    brand: &'a syn::Ident,
    errors: Vec<syn::Error>,
}

impl<'a> Lower<'a> {
    pub(crate) fn lower(
        brand: &'a syn::Ident,
        block: &mut syn::Block,
    ) -> Result<(), Vec<syn::Error>> {
        let mut lower = Self::new(brand);
        lower.visit_block_mut(block);
        lower.finish()
    }

    pub(crate) fn errors(errors: impl IntoIterator<Item = syn::Error>) -> proc_macro2::TokenStream {
        errors
            .into_iter()
            .map(|error| error.to_compile_error())
            .collect()
    }

    fn new(brand: &'a syn::Ident) -> Self {
        Self {
            brand,
            errors: Vec::new(),
        }
    }

    fn finish(self) -> Result<(), Vec<syn::Error>> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }

    fn contains_await(tokens: proc_macro2::TokenStream) -> bool {
        use proc_macro2::TokenTree;
        tokens.into_iter().any(|token| match token {
            TokenTree::Group(group) => Self::contains_await(group.stream()),
            TokenTree::Ident(ident) => ident == "await",
            _ => false,
        })
    }

    fn path(node: &syn::Macro) -> Option<(&'static str, String)> {
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

    fn expressions(&mut self, node: &mut syn::Macro) -> syn::Result<()> {
        let parser = punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated;
        let mut expressions = parser.parse2(node.tokens.clone())?;
        for expression in &mut expressions {
            self.visit_expr_mut(expression);
        }
        node.tokens = quote::quote! { #expressions };
        Ok(())
    }

    fn matches(&mut self, node: &mut syn::Macro) -> syn::Result<()> {
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
            Some(guard) => quote::quote! { #expr #comma #pat if #guard },
            None => quote::quote! { #expr #comma #pat },
        };
        Ok(())
    }

    fn vec(&mut self, node: &mut syn::Macro) -> syn::Result<()> {
        if node.tokens.is_empty() {
            return Ok(());
        }
        match syn::parse2::<VecInput>(node.tokens.clone())? {
            VecInput::List(mut expressions) => {
                for expression in &mut expressions {
                    self.visit_expr_mut(expression);
                }
                node.tokens = quote::quote! { #expressions };
            }
            VecInput::Repeat(mut value, semi, mut len) => {
                self.visit_expr_mut(&mut value);
                self.visit_expr_mut(&mut len);
                node.tokens = quote::quote! { #value #semi #len };
            }
        }
        Ok(())
    }

    fn reject_opaque(&mut self, node: &syn::Macro) {
        self.errors.push(syn::Error::new_spanned(
            node,
            "cannot lower `.await` inside this macro; hoist the await into a `let` \
             binding before the macro, or use a supported ::std/::core macro",
        ));
    }

    fn reject_async(&mut self, node: &impl quote::ToTokens) {
        self.errors.push(syn::Error::new_spanned(
            node,
            "independent async scope is not a fiber",
        ));
    }
}

impl visit_mut::VisitMut for Lower<'_> {
    fn visit_expr_await_mut(&mut self, node: &mut syn::ExprAwait) {
        use std::mem::replace;

        use syn::parse_quote_spanned;
        self.visit_expr_mut(&mut node.base);
        let brand = self.brand;
        let span = node.base.span();
        let base = replace(&mut *node.base, syn::parse_quote! { () });
        *node.base = parse_quote_spanned! {span=>
            #brand.awaitable(#base)
        };
    }

    fn visit_expr_async_mut(&mut self, node: &mut syn::ExprAsync) {
        self.reject_async(node);
    }

    fn visit_expr_closure_mut(&mut self, node: &mut syn::ExprClosure) {
        if node.asyncness.is_some() {
            self.reject_async(node);
        } else {
            use syn::visit_mut::visit_expr_closure_mut;
            visit_expr_closure_mut(self, node);
        }
    }

    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        if node.sig.asyncness.is_some() {
            self.reject_async(node);
        } else {
            use syn::visit_mut::visit_item_fn_mut;
            visit_item_fn_mut(self, node);
        }
    }

    fn visit_impl_item_fn_mut(&mut self, node: &mut syn::ImplItemFn) {
        if node.sig.asyncness.is_some() {
            self.reject_async(node);
        } else {
            use syn::visit_mut::visit_impl_item_fn_mut;
            visit_impl_item_fn_mut(self, node);
        }
    }

    fn visit_trait_item_fn_mut(&mut self, node: &mut syn::TraitItemFn) {
        if node.sig.asyncness.is_some() {
            self.reject_async(node);
        } else {
            use syn::visit_mut::visit_trait_item_fn_mut;
            visit_trait_item_fn_mut(self, node);
        }
    }

    fn visit_macro_mut(&mut self, node: &mut syn::Macro) {
        let path = Self::path(node);
        let is_pin = match &path {
            Some((_, name)) => name == "pin",
            None => node.path.segments.len() == 1 && node.path.segments[0].ident == "pin",
        };
        if is_pin {
            self.errors.push(syn::Error::new_spanned(
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
