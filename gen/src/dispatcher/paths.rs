pub(super) struct Paths {
    pub(super) core: syn::Path,
    pub(super) manifold: syn::Path,
    pub(super) runtime: syn::Path,
    pub(super) region: syn::Path,
}

impl Paths {
    pub(super) fn parse(attributes: &[syn::Attribute]) -> Result<Self, syn::Error> {
        let mut paths = Self::facade(syn::parse_quote!(::dope));
        for attribute in attributes {
            if !attribute.path().is_ident("dispatcher") {
                continue;
            }
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("crate") {
                    paths = Self::facade(meta.value()?.parse()?);
                } else if meta.path.is_ident("core") {
                    paths.core = meta.value()?.parse()?;
                } else if meta.path.is_ident("manifold") {
                    paths.manifold = meta.value()?.parse()?;
                } else if meta.path.is_ident("runtime") {
                    paths.runtime = meta.value()?.parse()?;
                } else if meta.path.is_ident("region") {
                    paths.region = meta.value()?.parse()?;
                } else {
                    return Err(meta.error(
                        "unknown `dispatcher` option; expected `crate`, `core`, `manifold`, `runtime`, or `region`",
                    ));
                }
                Ok(())
            })?;
        }
        Ok(paths)
    }

    fn facade(facade: syn::Path) -> Self {
        Self {
            core: syn::parse_quote!(#facade::core),
            manifold: syn::parse_quote!(#facade::manifold),
            runtime: syn::parse_quote!(#facade::runtime),
            region: syn::parse_quote!(::o3::cell::region::Token),
        }
    }
}
