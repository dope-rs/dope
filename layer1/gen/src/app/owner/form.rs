use syn::{GenericArgument, PathArguments, Type, spanned::Spanned as _};

use crate::{Failure, failure::App};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Kind {
    Required,
    Optional,
}

pub(super) struct Form {
    kind: Kind,
    inner: Type,
}

impl Form {
    pub(super) fn required(ty: &Type) -> Self {
        Self {
            kind: Kind::Required,
            inner: ty.clone(),
        }
    }

    pub(super) fn optional(ty: &Type) -> Result<Self, Failure> {
        let inner = Self::parse_option(ty)?;
        Ok(Self {
            kind: Kind::Optional,
            inner: inner.clone(),
        })
    }

    pub(super) const fn kind(&self) -> Kind {
        self.kind
    }

    pub(super) const fn inner(&self) -> &Type {
        &self.inner
    }

    fn segment(ty: &Type) -> Option<&syn::PathSegment> {
        let Type::Path(path) = ty else {
            return None;
        };
        if path.qself.is_some() {
            return None;
        }
        path.path.segments.last()
    }

    fn parse_option(ty: &Type) -> Result<&Type, Failure> {
        let Some(segment) = Self::segment(ty) else {
            return Err(Failure::at(ty.span(), App::OptionalType));
        };
        let PathArguments::AngleBracketed(args) = &segment.arguments else {
            return Err(Failure::at(ty.span(), App::OptionalType));
        };
        if segment.ident != "Option" || args.args.len() != 1 {
            return Err(Failure::at(ty.span(), App::OptionalType));
        }
        let Some(GenericArgument::Type(inner)) = args.args.first() else {
            return Err(Failure::at(ty.span(), App::OptionalType));
        };
        Ok(inner)
    }
}
