mod options;

use options::Options;
use syn::ext::IdentExt as _;

use crate::{
    Failure,
    app::{Owner, Spec, State},
    failure::App,
    manifold::{Brand, Inference},
};

/// Validates every field before classifying aggregate ownership failures.
pub(super) fn parse(mut input: syn::DeriveInput) -> Result<Spec, Failure> {
    use syn::{Data, Fields};

    use crate::manifold::attr;

    let source = quote::ToTokens::to_token_stream(&input);
    crate::receiver::Receiver::apply(&mut input);
    attr::check(&input.attrs)?;
    let (paths, driver, coord) = Options::parse(&input.attrs)?.into_parts();
    let data = match &input.data {
        Data::Struct(data) => data,
        Data::Enum(data) => {
            return Err(Failure::at(data.enum_token.span, App::Struct));
        }
        Data::Union(data) => {
            return Err(Failure::at(data.union_token.span, App::Struct));
        }
    };
    let named = match &data.fields {
        Fields::Named(named) => named,
        fields => {
            return Err(Failure::new(fields, App::Named));
        }
    };

    let brand = match driver {
        Some(lifetime) => Brand::explicit(&input.generics, &source, &lifetime)?,
        None => match Brand::infer(&input.generics, &source) {
            Inference::Selected(brand) => brand,
            Inference::Ambiguous => {
                return Err(Failure::new(&input.generics, App::DriverLifetime));
            }
        },
    };
    let mut owners = Vec::<Owner>::with_capacity(named.named.len());
    let mut states = Vec::new();
    let mut drain = None;
    let mut repeated = None;
    let mut repeated_route = None;
    let mut control = false;
    for field in &named.named {
        let visible = if let Some(state) = State::parse(field)? {
            states.push(state);
            true
        } else {
            let (owner, drains) = Owner::parse(field)?;
            if let Some(route) = owner.route()
                && owners
                    .iter()
                    .any(|previous| previous.route() == Some(route))
            {
                repeated_route.get_or_insert(field);
            }
            if drains && drain.replace(owners.len()).is_some() {
                repeated.get_or_insert(field);
            }
            let visible = owner.is_control();
            control |= visible;
            owners.push(owner);
            visible
        };
        if coord
            && visible
            && field
                .ident
                .as_ref()
                .is_some_and(|name| name.unraw() == "step")
        {
            return Err(Failure::new(field, App::CoordinateStep));
        }
    }
    if let Some(field) = repeated {
        return Err(Failure::new(field, App::Drain));
    }
    if let Some(field) = repeated_route {
        return Err(Failure::new(field, App::DuplicateRouteName));
    }
    if !coord && control {
        return Err(Failure::new(&input.ident, App::ControlCoordinate));
    }

    Ok(Spec {
        name: input.ident,
        generics: input.generics,
        owners,
        states,
        drain,
        brand,
        coordinate: coord,
        paths,
    })
}
