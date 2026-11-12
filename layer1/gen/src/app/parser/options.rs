use proc_macro2::TokenTree;

use crate::{
    Failure,
    failure::{App, Setting},
    paths::app,
    tokens::{self, Value},
};

pub(super) fn parse(
    attrs: &[syn::Attribute],
) -> Result<(app::App, Option<syn::Ident>, bool, bool), Failure> {
    let coord = coordinate(attrs)?;
    let mut attrs = attrs
        .iter()
        .filter(|attr| attr.path().is_ident("dispatcher"));
    let Some(attr) = attrs.next() else {
        return Ok((app::App::discover(), None, coord, false));
    };
    if let Some(extra) = attrs.next() {
        return Err(Failure::new(extra, App::Dispatcher));
    }
    let syn::Meta::List(list) = &attr.meta else {
        return Err(Failure::new(attr, App::DispatcherOptions));
    };
    if list.tokens.is_empty() {
        return Err(Failure::new(attr, App::DispatcherOptions));
    }

    let mut facade = None;
    let mut core = None;
    let mut manifold = None;
    let mut runtime = None;
    let mut driver = None;
    let mut lending = None;
    for value in tokens::values(list, |span| Failure::at(span, App::DispatcherOptions))? {
        if value.name == "crate" {
            set(&mut facade, value.value, &value.name, Setting::Crate)?;
        } else if value.name == "core" {
            set(&mut core, value.value, &value.name, Setting::Core)?;
        } else if value.name == "manifold" {
            set(&mut manifold, value.value, &value.name, Setting::Manifold)?;
        } else if value.name == "runtime" {
            set(&mut runtime, value.value, &value.name, Setting::Runtime)?;
        } else if value.name == "driver" {
            let selected = lifetime(&value)?;
            set(&mut driver, selected, &value.name, Setting::Driver)?;
        } else if value.name == "lending" {
            let selected = syn::parse2::<syn::LitBool>(value.value.tokens)
                .map_err(|_| Failure::new(&value.name, App::DispatcherOptions))?;
            set(&mut lending, selected.value, &value.name, Setting::Lending)?;
        } else {
            return Err(Failure::new(&value.name, App::UnknownDispatcher));
        }
    }

    let paths = if let Some(facade) = facade {
        if let Some(extra) = core.as_ref().or(manifold.as_ref()).or(runtime.as_ref()) {
            return Err(Failure::at(extra.span, App::FacadeMix));
        }
        app::App::from_facade(facade.tokens)
    } else if core.is_none() && manifold.is_none() && runtime.is_none() {
        app::App::discover()
    } else {
        let Some(core) = core else {
            return Err(Failure::new(attr, App::Core));
        };
        let Some(manifold) = manifold else {
            return Err(Failure::new(attr, App::Manifold));
        };
        let Some(runtime) = runtime else {
            return Err(Failure::new(attr, App::Runtime));
        };
        app::App::from_parts(core.tokens, manifold.tokens, runtime.tokens)
    };
    Ok((paths, driver, coord, lending.unwrap_or(false)))
}

fn coordinate(attrs: &[syn::Attribute]) -> Result<bool, Failure> {
    let mut attrs = attrs
        .iter()
        .filter(|attr| attr.path().is_ident("coordinate"));
    let Some(attr) = attrs.next() else {
        return Ok(false);
    };
    match &attr.meta {
        syn::Meta::Path(path) if path.is_ident("coordinate") => {}
        syn::Meta::Path(path) => return Err(Failure::new(path, App::Coordinate)),
        syn::Meta::List(list) => return Err(Failure::new(list, App::Coordinate)),
        syn::Meta::NameValue(value) => {
            return Err(Failure::new(value, App::Coordinate));
        }
    }
    if let Some(extra) = attrs.next() {
        return Err(Failure::new(extra, App::DuplicateCoordinate));
    }
    Ok(true)
}

fn lifetime(value: &Value) -> Result<syn::Ident, Failure> {
    let tokens: Vec<_> = value.value.tokens.clone().into_iter().collect();
    let [TokenTree::Punct(mark), TokenTree::Ident(name)] = tokens.as_slice() else {
        return Err(Failure::new(&value.name, App::DispatcherOptions));
    };
    if mark.as_char() != '\'' {
        return Err(Failure::at(mark.span(), App::DispatcherOptions));
    }
    Ok(name.clone())
}

fn set<T>(
    slot: &mut Option<T>,
    value: T,
    source: &syn::Ident,
    name: Setting,
) -> Result<(), Failure> {
    if slot.is_some() {
        return Err(Failure::new(source, App::Duplicate(name)));
    }
    *slot = Some(value);
    Ok(())
}
