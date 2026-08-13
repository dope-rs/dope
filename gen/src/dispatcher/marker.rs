pub(super) struct Marker {
    pub(super) lifetime: syn::Lifetime,
}

impl Marker {
    pub(super) fn parse(field: &syn::Field) -> Result<Self, syn::Error> {
        if field
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("manifold"))
        {
            return Err(syn::Error::new_spanned(
                field,
                "Application marker cannot also be a Manifold field",
            ));
        }
        if field
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("pin"))
        {
            return Err(syn::Error::new_spanned(
                field,
                "Application marker cannot be structurally pinned",
            ));
        }
        let Some(lifetime) = Self::invariant_lifetime(&field.ty) else {
            return Err(syn::Error::new_spanned(
                &field.ty,
                "an Application lifetime marker must be \
                 `::core::marker::PhantomData<fn(&'d ()) -> &'d ()>`",
            ));
        };
        Ok(Self { lifetime })
    }

    fn invariant_lifetime(ty: &syn::Type) -> Option<syn::Lifetime> {
        use syn::{GenericArgument, PathArguments, ReturnType, Type};

        let Type::Path(path) = ty else {
            return None;
        };
        if path.qself.is_some() || path.path.leading_colon.is_none() {
            return None;
        }
        let mut segments = path.path.segments.iter();
        let core = segments.next()?;
        let marker = segments.next()?;
        let phantom = segments.next()?;
        if segments.next().is_some()
            || core.ident != "core"
            || !matches!(core.arguments, PathArguments::None)
            || marker.ident != "marker"
            || !matches!(marker.arguments, PathArguments::None)
            || phantom.ident != "PhantomData"
        {
            return None;
        }
        let PathArguments::AngleBracketed(arguments) = &phantom.arguments else {
            return None;
        };
        let mut arguments = arguments.args.iter();
        let GenericArgument::Type(Type::FnPtr(function)) = arguments.next()? else {
            return None;
        };
        if arguments.next().is_some()
            || function.lifetimes.is_some()
            || function.unsafety.is_some()
            || function.abi.is_some()
            || function.variadic.is_some()
            || function.inputs.len() != 1
        {
            return None;
        }

        let input = function.inputs.first()?;
        if input.name.is_some() {
            return None;
        }
        let input_lifetime = Self::unit_reference_lifetime(&input.ty)?;
        let ReturnType::Type(_, output) = &function.output else {
            return None;
        };
        let output_lifetime = Self::unit_reference_lifetime(output)?;
        (input_lifetime == output_lifetime).then(|| input_lifetime.clone())
    }

    fn unit_reference_lifetime(ty: &syn::Type) -> Option<&syn::Lifetime> {
        let syn::Type::Reference(reference) = ty else {
            return None;
        };
        if reference.mutability.is_some() {
            return None;
        }
        let syn::Type::Tuple(tuple) = reference.elem.as_ref() else {
            return None;
        };
        if !tuple.elems.is_empty() {
            return None;
        }
        reference.lifetime.as_ref()
    }
}
