mod form;
mod role;

use form::{Form, Kind};
use proc_macro2::TokenTree;
use role::Role;
use syn::ext::IdentExt as _;

use crate::{Failure, failure::App};

#[derive(Clone, Copy)]
enum Attribute {
    Pin,
    Drain,
    Owner,
    Coordinate,
}

impl Attribute {
    fn matches(self, attr: &syn::Attribute) -> bool {
        let path = attr.path();
        match self {
            Self::Pin => path.is_ident("pin"),
            Self::Drain => path.is_ident("drain"),
            Self::Owner => path.is_ident("app") || path.is_ident("manifold"),
            Self::Coordinate => path.is_ident("coordinate"),
        }
    }

    fn get(self, attrs: &[syn::Attribute], skip: usize) -> Option<&syn::Attribute> {
        attrs.iter().filter(|attr| self.matches(attr)).nth(skip)
    }
}

fn options(
    paths: &[TokenTree],
    index: usize,
    app: bool,
    optional: bool,
    control: bool,
) -> Result<(bool, bool), Failure> {
    let Some(token) = paths.get(index) else {
        return Ok((optional, control));
    };
    let TokenTree::Ident(path) = token else {
        return Err(Failure::at(token.span(), App::UnknownOwner));
    };
    let next = match paths.get(index + 1) {
        None => paths.len(),
        Some(TokenTree::Punct(mark)) if mark.as_char() == ',' => index + 2,
        Some(token) => return Err(Failure::at(token.span(), App::UnknownOwner)),
    };
    if path == "optional" {
        if optional {
            return Err(Failure::new(path, App::StorageForm));
        }
        options(paths, next, app, true, control)
    } else if path == "control" {
        if app {
            return Err(Failure::new(path, App::ControlOwner));
        }
        if control {
            return Err(Failure::new(path, App::DuplicateControl));
        }
        options(paths, next, app, optional, true)
    } else {
        Err(Failure::new(path, App::UnknownOwner))
    }
}

pub(crate) struct Owner {
    name: syn::Ident,
    form: Form,
    role: Role,
}

impl Owner {
    pub(super) fn parse(field: &syn::Field) -> Result<(Self, bool), Failure> {
        let Some(name) = &field.ident else {
            return Err(Failure::new(field, App::Named));
        };
        if let Some(attr) = Attribute::Coordinate.get(&field.attrs, 0) {
            return Err(Failure::new(attr, App::CoordinateField));
        }
        let Some(pin) = Attribute::Pin.get(&field.attrs, 0) else {
            return Err(Failure::new(field, App::PinRequired));
        };
        if let Some(extra) = Attribute::Pin.get(&field.attrs, 1) {
            return Err(Failure::new(extra, App::DuplicatePin));
        }
        match &pin.meta {
            syn::Meta::Path(path) if path.is_ident("pin") => {}
            meta => {
                return Err(Failure::new(meta, App::PinShape));
            }
        }

        let drain = Attribute::Drain.get(&field.attrs, 0);
        if let Some(extra) = Attribute::Drain.get(&field.attrs, 1) {
            return Err(Failure::new(extra, App::DuplicateDrain));
        }
        if let Some(attr) = drain {
            match &attr.meta {
                syn::Meta::Path(path) if path.is_ident("drain") => {}
                meta => {
                    return Err(Failure::new(meta, App::DrainShape));
                }
            }
        }

        let Some(attr) = Attribute::Owner.get(&field.attrs, 0) else {
            return Err(Failure::new(field, App::OwnerRequired));
        };
        if let Some(extra) = Attribute::Owner.get(&field.attrs, 1) {
            return Err(Failure::new(extra, App::Lifecycle));
        }

        let (app, optional, control) = match &attr.meta {
            syn::Meta::Path(path) => (path.is_ident("app"), false, false),
            syn::Meta::List(list) if list.tokens.is_empty() => {
                return Err(Failure::new(list, App::EmptyOwner));
            }
            syn::Meta::List(list) => {
                let paths: Vec<_> = list.tokens.clone().into_iter().collect();
                let app = list.path.is_ident("app");
                let (optional, control) = options(&paths, 0, app, false, false)?;
                (app, optional, control)
            }
            syn::Meta::NameValue(value) => {
                return Err(Failure::new(value, App::OwnerShape));
            }
        };

        let form = if optional {
            Form::optional(&field.ty)?
        } else {
            Form::required(&field.ty)
        };
        if drain.is_some() && form.kind() != Kind::Required {
            return Err(Failure::new(field, App::DrainOwned));
        }
        let role = if app {
            Role::App
        } else {
            Role::Manifold {
                controlled: control,
                route: quote::format_ident!("{}_ROUTE", name.unraw().to_string().to_uppercase()),
            }
        };
        Ok((
            Self {
                name: name.clone(),
                form,
                role,
            },
            drain.is_some(),
        ))
    }

    pub(crate) fn name(&self) -> &syn::Ident {
        &self.name
    }

    pub(crate) fn inner(&self) -> &syn::Type {
        self.form.inner()
    }

    pub(crate) const fn is_optional(&self) -> bool {
        matches!(self.form.kind(), Kind::Optional)
    }

    pub(crate) const fn is_app(&self) -> bool {
        matches!(self.role, Role::App)
    }

    pub(crate) fn is_manifold(&self) -> bool {
        matches!(self.role, Role::Manifold { .. })
    }

    pub(crate) fn route(&self) -> Option<&syn::Ident> {
        match &self.role {
            Role::App => None,
            Role::Manifold { route, .. } => Some(route),
        }
    }

    pub(crate) const fn is_control(&self) -> bool {
        matches!(
            self.role,
            Role::Manifold {
                controlled: true,
                ..
            }
        )
    }
}
