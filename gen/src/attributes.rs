pub(crate) trait Attributes {
    fn reject_packed(&self) -> Result<(), syn::Error>;
}

impl Attributes for [syn::Attribute] {
    fn reject_packed(&self) -> Result<(), syn::Error> {
        for attr in self {
            use syn::{Meta, Token, punctuated::Punctuated};
            if !attr.path().is_ident("repr") {
                continue;
            }
            let reprs = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            if let Some(repr) = reprs.iter().find(|repr| match repr {
                Meta::Path(path) => path.is_ident("packed"),
                Meta::List(list) => list.path.is_ident("packed"),
                Meta::NameValue(value) => value.path.is_ident("packed"),
            }) {
                use syn::Error;
                return Err(Error::new_spanned(
                    repr,
                    "pinned projection does not support repr(packed)",
                ));
            }
        }
        Ok(())
    }
}
