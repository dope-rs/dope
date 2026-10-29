fn packed(attr: &syn::Attribute) -> Option<proc_macro2::Span> {
    let syn::Meta::List(list) = &attr.meta else {
        return None;
    };
    if !list.path.is_ident("repr") {
        return None;
    }
    list.tokens.clone().into_iter().find_map(|token| {
        let proc_macro2::TokenTree::Ident(name) = token else {
            return None;
        };
        if name == "packed" {
            Some(name.span())
        } else {
            None
        }
    })
}

pub(crate) fn check(attrs: &[syn::Attribute]) -> Result<(), Failure> {
    match attrs.iter().find_map(packed) {
        Some(span) => Err(Failure::at(span, Manifold::Packed)),
        None => Ok(()),
    }
}

use crate::{Failure, failure::Manifold};
