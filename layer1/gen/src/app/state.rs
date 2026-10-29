use proc_macro2::TokenTree;

use crate::{Failure, failure::App};

#[derive(Clone, Copy)]
enum Kind {
    Local,
    Schedule,
}

fn dispatcher(attrs: &[syn::Attribute], skip: usize) -> Option<&syn::Attribute> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("dispatcher"))
        .nth(skip)
}

fn conflict(attrs: &[syn::Attribute]) -> Option<&syn::Attribute> {
    attrs.iter().find(|attr| {
        let path = attr.path();
        path.is_ident("app")
            || path.is_ident("manifold")
            || path.is_ident("drain")
            || path.is_ident("pin")
            || path.is_ident("coordinate")
    })
}

pub(crate) struct State {
    name: syn::Ident,
    ty: syn::Type,
    kind: Kind,
}

impl State {
    pub(super) fn parse(field: &syn::Field) -> Result<Option<Self>, Failure> {
        let Some(attr) = dispatcher(&field.attrs, 0) else {
            return Ok(None);
        };
        if let Some(extra) = dispatcher(&field.attrs, 1) {
            return Err(Failure::new(extra, App::DuplicateState));
        }
        let list = match &attr.meta {
            syn::Meta::List(list) => list,
            syn::Meta::Path(path) => {
                return Err(Failure::new(path, App::StateShape));
            }
            syn::Meta::NameValue(value) => {
                return Err(Failure::new(value, App::StateShape));
            }
        };

        let mut roles = list.tokens.clone().into_iter();
        let Some(TokenTree::Ident(role)) = roles.next() else {
            return Err(Failure::new(attr, App::StateRole));
        };
        if roles.next().is_some() {
            return Err(Failure::new(attr, App::StateRole));
        }
        let kind = if role == "state" {
            Kind::Local
        } else if role == "schedule" {
            Kind::Schedule
        } else {
            return Err(Failure::new(&role, App::StateName));
        };

        if let Some(conflict) = conflict(&field.attrs) {
            return Err(Failure::new(conflict, App::StateConflict));
        }
        let Some(name) = &field.ident else {
            return Err(Failure::new(field, App::Named));
        };
        Ok(Some(Self {
            name: name.clone(),
            ty: field.ty.clone(),
            kind,
        }))
    }

    pub(crate) fn name(&self) -> &syn::Ident {
        &self.name
    }

    pub(crate) fn ty(&self) -> &syn::Type {
        &self.ty
    }

    pub(crate) const fn is_scheduled(&self) -> bool {
        matches!(self.kind, Kind::Schedule)
    }
}
