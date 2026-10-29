mod output;
mod owner;
mod parser;
mod state;

use output::{control::Control, drain, routes, runtime};
pub(super) use owner::Owner;
pub(super) use state::State;

use crate::{manifold::Brand, paths::app};

pub(crate) struct Spec {
    name: syn::Ident,
    generics: syn::Generics,
    owners: Vec<Owner>,
    states: Vec<State>,
    drain: Option<usize>,
    brand: Brand,
    coordinate: bool,
    paths: app::App,
}

impl Spec {
    pub(crate) fn expand(input: syn::DeriveInput) -> proc_macro::TokenStream {
        let spec = match parser::parse(input) {
            Ok(spec) => spec,
            Err(failure) => return failure.emit(),
        };
        let routes = routes::generate(&spec);
        let control = Control::new(&spec);
        let (fields, view) = match control.fields() {
            Ok(fields) => fields,
            Err(failure) => return failure.emit(),
        };
        let drain = drain::generate(&spec);
        let runtime = runtime::generate(&spec, &control);
        quote::quote! {
            #routes
            #fields
            const _: () = {
                #view
                #drain
                #runtime
            };
        }
        .into()
    }

    pub(crate) fn drain(&self) -> Option<&Owner> {
        self.drain.and_then(|index| self.owners.get(index))
    }
}
