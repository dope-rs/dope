#[derive(Clone, Copy)]
pub(super) enum Role {
    State,
    Schedule,
    Marker,
}

impl Role {
    pub(super) fn parse(field: &syn::Field) -> Result<Option<Self>, syn::Error> {
        let mut attributes = field
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("dispatcher"));
        let Some(attribute) = attributes.next() else {
            return Ok(None);
        };
        if let Some(extra) = attributes.next() {
            return Err(syn::Error::new_spanned(
                extra,
                "Application fields accept one `#[dispatcher(...)]` attribute",
            ));
        }

        let mut role = None;
        attribute.parse_nested_meta(|meta| {
            if role.is_some() {
                return Err(meta.error("Application fields require exactly one role"));
            }
            role = if meta.path.is_ident("state") {
                Some(Self::State)
            } else if meta.path.is_ident("schedule") {
                Some(Self::Schedule)
            } else if meta.path.is_ident("marker") {
                Some(Self::Marker)
            } else {
                return Err(meta.error("expected `state`, `schedule`, or `marker`"));
            };
            Ok(())
        })?;

        role.map(Some).ok_or_else(|| {
            syn::Error::new_spanned(
                attribute,
                "expected `#[dispatcher(state)]`, `#[dispatcher(schedule)]`, or \
                 `#[dispatcher(marker)]`",
            )
        })
    }
}
