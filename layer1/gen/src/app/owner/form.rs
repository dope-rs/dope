use syn::{GenericArgument, PathArguments, Type, spanned::Spanned as _};

use crate::{Failure, failure::App};

fn segment(ty: &Type) -> Option<&syn::PathSegment> {
    let Type::Path(path) = ty else {
        return None;
    };
    if path.qself.is_some() {
        return None;
    }
    path.path.segments.last()
}

pub(super) fn parse_option(ty: &Type) -> Result<&Type, Failure> {
    let Some(segment) = segment(ty) else {
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
