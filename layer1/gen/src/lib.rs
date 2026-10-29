extern crate proc_macro;

mod app;
mod failure;
mod fiber;
mod manifold;
mod paths;
mod receiver;
mod tokens;
mod work;

pub(crate) use failure::Failure;

use crate::{app::Spec, fiber::macros};

/// Lowers an `async fn` into a driver-scoped fiber.
/// Every argument pattern must bind a name; use `name @ pattern` for an
/// otherwise unnamed argument. The fiber owns complete arguments before
/// applying their patterns.
#[proc_macro_attribute]
pub fn fiber_fn(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    use syn::ItemFn;

    let args = syn::parse_macro_input!(attr as macros::Args);
    let item = syn::parse_macro_input!(input as ItemFn);
    match macros::Attribute::new(args, item).try_expand() {
        Ok(tokens) => tokens,
        Err(failure) => failure.emit(),
    }
}

/// Lowers an `async move` block into a driver-scoped fiber.
/// Only fibers may be awaited; immediately awaited non-move `async` blocks
/// share the enclosing fiber's brand and application budget.
#[proc_macro]
pub fn fiber(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as macros::Macro);
    match input.try_expand() {
        Ok(tokens) => tokens,
        Err(failure) => {
            use proc_macro::{Delimiter, Group, TokenTree};

            TokenTree::Group(Group::new(Delimiter::Brace, failure.emit())).into()
        }
    }
}

/// Implements exact pinned manifold forwarding for a single-field wrapper.
#[proc_macro_derive(Forward, attributes(forward))]
pub fn forward(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    manifold::expand(input)
}

/// Implements a statically dispatched runtime application.
/// Every retained manifold owner must be structurally pinned.
/// Generated route constants use the manifold field's uppercase name plus `_ROUTE`;
/// those generated names must be distinct.
/// `#[coordinate]` reserves `<snake_case_app>_coord::Fields` for the coordinate view.
/// The view alias requires explicit generic arguments, including parameters
/// with defaults on the application type.
#[proc_macro_derive(Application, attributes(app, coordinate, dispatcher, drain, manifold))]
pub fn application(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    Spec::expand(input)
}

/// Generates a minimal fair selector from a fieldless work enum.
#[proc_macro_derive(WorkGraph, attributes(work))]
pub fn work_graph(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    work::expand(input)
}
