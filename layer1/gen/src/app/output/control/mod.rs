mod fields;
mod members;
mod park;

use crate::{Failure, app::Spec};

pub(crate) struct Control<'a> {
    spec: &'a Spec,
    module: syn::Ident,
    helper: syn::Ident,
    seal: syn::Ident,
    root: syn::Ident,
}

impl<'a> Control<'a> {
    pub(crate) fn new(spec: &'a Spec) -> Self {
        Self {
            spec,
            module: Self::module(spec),
            helper: spec.brand.ident(&format!("__{}Fields", spec.name)),
            seal: spec.brand.ident("__coordinate_seal"),
            root: spec.brand.ident("root"),
        }
    }

    pub(crate) fn fields(
        &self,
    ) -> Result<(proc_macro2::TokenStream, proc_macro2::TokenStream), Failure> {
        if !self.spec.coordinate {
            return Ok((quote::quote! {}, quote::quote! {}));
        }
        fields::emit(
            self.spec,
            &self.module,
            &self.helper,
            &self.seal,
            &self.root,
        )
    }

    pub(crate) fn park(&self) -> proc_macro2::TokenStream {
        if !self.spec.coordinate {
            return quote::quote! {};
        }
        park::emit(self.spec, &self.helper, &self.seal, &self.root)
    }

    fn module(spec: &Spec) -> syn::Ident {
        let source = spec.name.to_string();
        let chars = source.chars().collect::<Vec<_>>();
        let mut snake = String::with_capacity(source.len() + 6);
        for (index, current) in chars.iter().copied().enumerate() {
            let previous = index.checked_sub(1).and_then(|at| chars.get(at)).copied();
            let next = chars.get(index + 1).copied();
            let boundary = current.is_uppercase()
                && (previous.is_some_and(|value| value.is_lowercase() || value.is_numeric())
                    || previous.is_some_and(char::is_uppercase)
                        && next.is_some_and(char::is_lowercase));
            if boundary {
                snake.push('_');
            }
            snake.extend(current.to_lowercase());
        }
        quote::format_ident!("{}_coord", snake)
    }
}
