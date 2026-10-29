use proc_macro2::TokenStream;

use crate::{
    Failure,
    failure::{self, Manifold},
    manifold, paths, tokens,
};

struct Source<'a> {
    field: &'a syn::Ident,
    field_ty: &'a syn::Type,
    args: Option<&'a syn::MetaList>,
}

fn facade(input: &syn::DeriveInput) -> Result<Option<TokenStream>, Failure> {
    let mut attrs = input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("forward"));
    let Some(attr) = attrs.next() else {
        return Ok(None);
    };
    if let Some(extra) = attrs.next() {
        return Err(Failure::new(extra, Manifold::DuplicateCrate));
    }
    let syn::Meta::List(list) = &attr.meta else {
        return Err(Failure::new(attr, Manifold::Crate));
    };
    let mut values = tokens::values(list, |span| Failure::at(span, Manifold::Crate))?.into_iter();
    let Some(value) = values.next() else {
        return Err(Failure::new(attr, Manifold::Crate));
    };
    if value.name != "crate" || values.next().is_some() {
        return Err(Failure::new(&value.name, Manifold::Crate));
    }
    Ok(Some(value.value.tokens))
}

fn marked(attrs: &[syn::Attribute]) -> Result<Option<&syn::Attribute>, Failure> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("forward"))
        .try_fold(None, |first, attr| {
            if first.is_some() {
                Err(Failure::new(attr, Manifold::Mark))
            } else {
                Ok(Some(attr))
            }
        })
}

pub(crate) fn expand(input: syn::DeriveInput) -> proc_macro::TokenStream {
    match generate(input) {
        Ok(tokens) => tokens.into(),
        Err(failure) => failure.emit(),
    }
}

fn generate(mut input: syn::DeriveInput) -> Result<TokenStream, Failure> {
    let tokens = quote::ToTokens::to_token_stream(&input);
    crate::receiver::Receiver::apply(&mut input);
    let facade = facade(&input)?;
    let source = Source::new(&input)?;
    let brand = source.brand(&input, &tokens)?;
    let [vturn, vctx] = ["shared", "exclusive"].map(|name| brand.ident(name));
    let paths = paths::Forward::resolve(facade);
    let name = &input.ident;
    let args = input.generics.split_for_impl().1;
    let bounds = input
        .generics
        .where_clause
        .iter()
        .flat_map(|clause| clause.predicates.iter());
    let proj = bounds.clone();
    let field = source.field;
    let ty = source.field_ty;
    let life = brand.lifetime();
    let core = paths.core();
    let mf = paths.manifold();
    let generics = brand.generics(&input.generics);
    let head = generics.split_for_impl().0;
    let owner = brand.fresh("__owner");
    let turn = brand.fresh("__turn");
    let root = brand.fresh("__root");
    let mut projection = input.generics.clone();
    projection.params.insert(0, syn::parse_quote!(#owner));
    let (projection_head, _, projection_clause) = projection.split_for_impl();

    Ok(quote::quote! {
        const _: () = {
            fn #vturn #projection_head(
                #vturn: ::core::pin::Pin<&#owner #name #args>,
            ) -> ::core::pin::Pin<&#owner #ty> #projection_clause {
                #vturn.project_ref().#field
            }

            fn #vctx #projection_head(
                #vctx: ::core::pin::Pin<&#owner mut #name #args>,
            ) -> ::core::pin::Pin<&#owner mut #ty> #projection_clause {
                #vctx.project().#field
            }

            /// SAFETY: every projection forwards to the single wrapped field,
            /// so the wrapper stays pinned exactly when that field does and
            /// no borrowed lifecycle context can escape the call.
            unsafe impl #head #mf::app::dispatch::raw::Manifold<#life> for #name #args
            where
                #ty: #mf::app::dispatch::raw::Manifold<#life>,
                #(#bounds,)*
            {
                type Route = <#ty as #mf::app::dispatch::raw::Manifold<#life>>::Route;
                const COORDS: ::core::primitive::usize = <#ty as #mf::app::dispatch::raw::Manifold<#life>>::COORDS;
                type Dispatch = #mf::app::dispatch::raw::Projected<Self>;
                type Install = #mf::app::dispatch::raw::Projected<Self>;
                type Finish = #mf::app::dispatch::raw::Projected<Self>;
                type Activate = #mf::app::dispatch::raw::Projected<Self>;
                type Probe = #mf::app::dispatch::raw::Projected<Self>;
                type PrePark = <#ty as #mf::app::dispatch::raw::Manifold<#life>>::PrePark;
                type Shutdown = <#ty as #mf::app::dispatch::raw::Manifold<#life>>::Shutdown;

                /// SAFETY: the enclosing exact dispatcher installed this pinned
                /// owner, and the body forwards that same projection.
                unsafe fn pre_park<#owner, #turn, #root>(
                    mut self: ::core::pin::Pin<&#owner mut Self>,
                    #vturn: #core::driver::schedule::Turn<#turn, #life>,
                    #vctx: &mut #mf::app::dispatch::raw::Context<'_, #root, #life, Self::PrePark>,
                ) -> ::core::pin::Pin<&#owner mut Self>
                where
                    Self: #root,
                {
                    /// SAFETY: Forward::pre_park inherits the installed-owner precondition.
                    unsafe { #mf::app::dispatch::raw::Manifold::pre_park(
                        #mf::app::dispatch::raw::Projection::inner_mut(self.as_mut()),
                        #vturn,
                        #vctx,
                    ) };
                    self
                }
                fn shutdown<#owner, #turn, #root>(
                    mut self: ::core::pin::Pin<&#owner mut Self>,
                    #vturn: #core::driver::schedule::Turn<#turn, #life>,
                    #vctx: &mut #mf::app::dispatch::raw::Context<'_, #root, #life, Self::Shutdown>,
                ) -> ::core::pin::Pin<&#owner mut Self>
                where
                    Self: #root,
                {
                    #mf::app::dispatch::raw::Manifold::shutdown(
                        #mf::app::dispatch::raw::Projection::inner_mut(self.as_mut()),
                        #vturn,
                        #vctx,
                    );
                    self
                }
            }

            impl #head #mf::app::dispatch::raw::Projection<#life> for #name #args
            where
                #(#proj,)*
            {
                type Inner = #ty;

                fn inner(
                    self: ::core::pin::Pin<&Self>,
                ) -> ::core::pin::Pin<&Self::Inner> {
                    #vturn(self)
                }

                fn inner_mut(
                    self: ::core::pin::Pin<&mut Self>,
                ) -> ::core::pin::Pin<&mut Self::Inner> {
                    #vctx(self)
                }
            }
        };
    })
}

impl<'a> Source<'a> {
    fn new(input: &'a syn::DeriveInput) -> Result<Self, Failure> {
        use syn::{Data, Fields, Meta};

        use crate::manifold::attr;

        attr::check(&input.attrs)?;
        let name = &input.ident;
        let data = match &input.data {
            Data::Struct(data) => data,
            Data::Enum(data) => {
                return Err(Failure::new(&data.enum_token, Manifold::Struct));
            }
            Data::Union(data) => {
                return Err(Failure::new(&data.union_token, Manifold::Struct));
            }
        };
        let fields = match &data.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(fields) => {
                return Err(Failure::new(fields, Manifold::Named));
            }
            Fields::Unit => {
                return Err(Failure::new(name, Manifold::Named));
            }
        };
        let mut fields = fields.iter();
        let Some(item) = fields.next() else {
            return Err(Failure::new(name, Manifold::FieldCount));
        };
        if let Some(extra) = fields.next() {
            return Err(Failure::new(extra, Manifold::FieldCount));
        }
        let Some(field) = &item.ident else {
            return Err(Failure::new(item, Manifold::NamedField));
        };
        let Some(attr) = marked(&item.attrs)? else {
            return Err(Failure::new(field, Manifold::FieldMark));
        };
        if !item.attrs.iter().any(|attr| attr.path().is_ident("pin")) {
            return Err(Failure::new(field, Manifold::FieldPin));
        }
        let args = match &attr.meta {
            Meta::Path(_) => None,
            Meta::List(list) => Some(list),
            Meta::NameValue(value) => {
                return Err(Failure::new(value, Manifold::ForwardArgs));
            }
        };
        Ok(Self {
            field,
            field_ty: &item.ty,
            args,
        })
    }

    fn brand(
        &self,
        input: &syn::DeriveInput,
        tokens: &TokenStream,
    ) -> Result<manifold::Brand, Failure> {
        use syn::{Token, punctuated::Punctuated};

        use crate::manifold::{Brand, Inference};

        let lifetime = match self.args {
            Some(list) => {
                let mut args = list
                    .parse_args_with(Punctuated::<syn::Lifetime, Token![,]>::parse_terminated)
                    .map_err(|source| Failure::Syntax {
                        source,
                        kind: failure::Kind::Manifold(Manifold::ForwardArgs),
                    })?;
                let lifetime = args.pop();
                if let Some(extra) = args.pop() {
                    return Err(Failure::new(&extra, Manifold::Driver));
                }
                lifetime
            }
            None => None,
        };
        match lifetime {
            Some(life) => Brand::explicit(&input.generics, tokens, &life.ident),
            None => match Brand::infer(&input.generics, tokens) {
                Inference::Selected(brand) => Ok(brand),
                Inference::Ambiguous => Err(Failure::new(&input.generics, Manifold::Driver)),
            },
        }
    }
}
