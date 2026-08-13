pub(super) struct State {
    pub(super) name: syn::Ident,
    pub(super) ty: syn::Type,
    pub(super) schedule: bool,
}

impl State {
    pub(super) fn parse(field: &syn::Field, schedule: bool) -> Result<Self, syn::Error> {
        if field
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("manifold"))
        {
            return Err(syn::Error::new_spanned(
                field,
                "Application state cannot also be a Manifold field",
            ));
        }
        if field
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("pin"))
        {
            return Err(syn::Error::new_spanned(
                field,
                "Application state cannot be structurally pinned",
            ));
        }
        let name = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new_spanned(field, "Application requires named fields"))?;
        Ok(Self {
            name,
            ty: field.ty.clone(),
            schedule,
        })
    }
}
