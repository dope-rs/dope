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
use syn::{DeriveInput, parse_macro_input};

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
