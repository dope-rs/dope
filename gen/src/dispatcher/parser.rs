use crate::{attributes, brand, dispatcher, field};

pub(super) struct Parser {
    input: syn::DeriveInput,
}

impl Parser {
    pub(super) fn new(input: syn::DeriveInput) -> Self {
        Self { input }
    }

    pub(super) fn parse(self) -> Result<dispatcher::Application, syn::Error> {
        use dispatcher::ParseError;
        use syn::{Data, Fields};

        let input = self.input;
        attributes::Attributes::reject_packed(input.attrs.as_slice())?;
        let coordinate = input.attrs.iter().any(|a| a.path().is_ident("coordinate"));
        let paths = {
            use dispatcher::paths;
            paths::Paths::parse(&input.attrs)?
        };
        let name = input.ident;
        let generics = input.generics;
        let data = match &input.data {
            Data::Struct(structure) => structure,
            _ => {
                return Err(ParseError::new_spanned(
                    &name,
                    "Application requires a struct",
                ));
            }
        };
        let named = match &data.fields {
            Fields::Named(named) => named,
            _ => {
                return Err(ParseError::new_spanned(
                    &name,
                    "Application requires named fields",
                ));
            }
        };
        let mut fields = Vec::with_capacity(named.named.len());
        let mut states = Vec::new();
        let mut marker_lifetime = None;
        for candidate in &named.named {
            use dispatcher::{role, state};

            match role::Role::parse(candidate)? {
                Some(role::Role::State) => {
                    states.push(state::State::parse(candidate, false)?);
                    continue;
                }
                Some(role::Role::Schedule) => {
                    states.push(state::State::parse(candidate, true)?);
                    continue;
                }
                Some(role::Role::Marker) => {
                    use dispatcher::marker;

                    let marker = marker::Marker::parse(candidate)?;
                    if marker_lifetime.replace(marker.lifetime).is_some() {
                        return Err(ParseError::new_spanned(
                            candidate,
                            "Application accepts one lifetime marker",
                        ));
                    }
                    continue;
                }
                None => {}
            }
            let ident = candidate.ident.clone().ok_or_else(|| {
                ParseError::new_spanned(candidate, "Application requires named fields")
            })?;
            let ty = candidate.ty.clone();
            let pinned = candidate
                .attrs
                .iter()
                .any(|attribute| attribute.path().is_ident("pin"));
            let mut attributes = candidate
                .attrs
                .iter()
                .filter(|attribute| attribute.path().is_ident("manifold"));
            let Some(attribute) = attributes.next() else {
                return Err(ParseError::new_spanned(
                    candidate,
                    "Application fields must be marked `#[manifold]`",
                ));
            };
            if let Some(extra) = attributes.next() {
                return Err(ParseError::new_spanned(
                    extra,
                    "Application fields accept one `#[manifold]` attribute",
                ));
            }
            if !pinned {
                return Err(ParseError::new_spanned(
                    candidate,
                    "Application fields must be marked `#[pin]`",
                ));
            }
            let mut optional = false;
            let mut control = false;
            let mut borrowed = false;
            if !matches!(attribute.meta, syn::Meta::Path(_)) {
                attribute.parse_nested_meta(|meta| {
                    if meta.path.is_ident("optional") {
                        optional = true;
                        Ok(())
                    } else if meta.path.is_ident("control") {
                        control = true;
                        Ok(())
                    } else if meta.path.is_ident("borrowed") {
                        borrowed = true;
                        Ok(())
                    } else {
                        Err(meta.error("unknown `manifold` option"))
                    }
                })?;
            }
            if borrowed && (optional || control) {
                return Err(ParseError::new_spanned(
                    attribute,
                    "a borrowed Manifold field cannot be optional or controlled",
                ));
            }
            if borrowed && !Self::is_anchor(&ty) {
                return Err(ParseError::new_spanned(
                    &ty,
                    "a borrowed Manifold field must have type `client::Anchor<'_, M>`",
                ));
            }
            let const_ident = quote::format_ident!("{}_ROUTE", ident.to_string().to_uppercase());
            fields.push(field::Field {
                name: ident,
                ty,
                optional,
                control,
                borrowed,
                pinned,
                const_ident,
            });
        }

        let brand = match marker_lifetime {
            Some(lifetime) => brand::Brand::explicit(&generics, lifetime)?,
            None if generics.lifetimes().count() > 1 => {
                return Err(ParseError::new_spanned(
                    &generics,
                    "an Application with multiple lifetime parameters requires an explicit \
                     invariant `#[dispatcher(marker)]` field",
                ));
            }
            None => brand::Brand::infer(&generics),
        };

        if !coordinate && fields.iter().any(|field| field.control) {
            return Err(ParseError::new_spanned(
                &name,
                "`#[manifold(control)]` requires `#[coordinate]` on the Application",
            ));
        }

        Ok(dispatcher::Application {
            name,
            generics,
            fields,
            states,
            brand,
            coordinate,
            paths,
        })
    }

    fn is_anchor(ty: &syn::Type) -> bool {
        use syn::{GenericArgument, PathArguments, Type};

        let Type::Path(path) = ty else {
            return false;
        };
        let Some(segment) = path.path.segments.last() else {
            return false;
        };
        let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
            return false;
        };
        segment.ident == "Anchor"
            && arguments.args.len() == 2
            && matches!(arguments.args.first(), Some(GenericArgument::Lifetime(_)))
            && matches!(arguments.args.last(), Some(GenericArgument::Type(_)))
    }
}
