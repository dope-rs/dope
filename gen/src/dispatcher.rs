use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Error, Fields, FieldsNamed, GenericArgument, Generics, Ident, Meta,
    PathArguments, Type,
};

use crate::derive::{DeriveAttrs, DeriveGenerics};

struct ManifoldField {
    name: Ident,
    ty: Type,
    optional: bool,
    pinned: bool,
    const_ident: Ident,
}

impl ManifoldField {
    fn inner_ty(&self) -> proc_macro2::TokenStream {
        if self.optional {
            if let Type::Path(tp) = &self.ty
                && let Some(seg) = tp.path.segments.last()
                && seg.ident == "Option"
                && let PathArguments::AngleBracketed(args) = &seg.arguments
            {
                for a in &args.args {
                    if let GenericArgument::Type(inner) = a {
                        return quote! { #inner };
                    }
                }
            }
            let ty = &self.ty;
            quote! { #ty }
        } else {
            let ty = &self.ty;
            quote! { #ty }
        }
    }

    fn wrap_body(
        &self,
        body_with: impl FnOnce(proc_macro2::TokenStream) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let field = &self.name;
        let body = body_with(quote! { __m });
        if self.optional {
            quote! {{
                let __f = self.as_mut().project().#field;
                if let ::core::option::Option::Some(__m) = __f.as_pin_mut() {
                    #body
                }
            }}
        } else {
            quote! {{
                let __m = self.as_mut().project().#field;
                #body
            }}
        }
    }

    fn wrap_body_ref(
        &self,
        body_with: impl FnOnce(proc_macro2::TokenStream) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let field = &self.name;
        let body = body_with(quote! { __m });
        if self.optional {
            quote! {{
                let __f = self.project_ref().#field;
                if let ::core::option::Option::Some(__m) = __f.as_pin_ref() {
                    #body
                }
            }}
        } else {
            quote! {{
                let __m = self.project_ref().#field;
                #body
            }}
        }
    }
}

pub(crate) struct DispatcherSpec {
    name: Ident,
    generics: Generics,
    fields: Vec<ManifoldField>,
    coordinate: bool,
}

impl DispatcherSpec {
    pub(crate) fn derive(input: DeriveInput) -> TokenStream {
        if let Err(error) = input.attrs.reject_packed() {
            return error.to_compile_error().into();
        }
        let name = input.ident;
        let generics = input.generics;
        let coordinate = input.attrs.iter().any(|a| a.path().is_ident("coordinate"));
        let data = match &input.data {
            Data::Struct(s) => s,
            _ => {
                return Error::new_spanned(&name, "Dispatcher requires a struct")
                    .to_compile_error()
                    .into();
            }
        };
        let named = match &data.fields {
            Fields::Named(n) => n,
            _ => {
                return Error::new_spanned(&name, "Dispatcher requires named fields")
                    .to_compile_error()
                    .into();
            }
        };
        let spec = match Self::parse(name, generics, named, coordinate) {
            Ok(s) => s,
            Err(e) => return e.to_compile_error().into(),
        };
        let route_consts = spec.route_consts();
        let manifold_impl = spec.manifold_impl();
        let projections_impl = spec.projections_impl();
        quote! {
            #route_consts
            #manifold_impl
            #projections_impl
        }
        .into()
    }

    fn parse(
        name: Ident,
        generics: Generics,
        named: &FieldsNamed,
        coordinate: bool,
    ) -> Result<Self, Error> {
        let mut tagged: Vec<(Ident, Type, bool, bool)> = Vec::new();
        let mut any_tagged = false;
        let mut all: Vec<(Ident, Type, bool)> = Vec::new();
        for f in &named.named {
            let ident = f
                .ident
                .clone()
                .ok_or_else(|| Error::new_spanned(f, "Dispatcher requires named fields"))?;
            let ty = f.ty.clone();
            let pinned = f.attrs.iter().any(|attr| attr.path().is_ident("pin"));
            all.push((ident.clone(), ty.clone(), pinned));
            let mut is_manifold = false;
            let mut optional = false;
            for attr in &f.attrs {
                if !attr.path().is_ident("manifold") {
                    continue;
                }
                is_manifold = true;
                if matches!(attr.meta, Meta::Path(_)) {
                    continue;
                }
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("optional") {
                        optional = true;
                        Ok(())
                    } else {
                        Err(meta.error("unknown `manifold` option"))
                    }
                })?;
            }
            if is_manifold {
                any_tagged = true;
                tagged.push((ident, ty, optional, pinned));
            }
        }

        let raw: Vec<(Ident, Type, bool, bool)> = if any_tagged {
            tagged
        } else {
            all.into_iter().map(|(i, t, p)| (i, t, false, p)).collect()
        };

        let mut fields = Vec::with_capacity(raw.len());
        for (ident, ty, optional, pinned) in raw {
            let const_ident = format_ident!("{}_ROUTE", ident.to_string().to_uppercase());
            fields.push(ManifoldField {
                name: ident,
                ty,
                optional,
                pinned,
                const_ident,
            });
        }

        Ok(Self {
            name,
            generics,
            fields,
            coordinate,
        })
    }

    fn route_consts(&self) -> proc_macro2::TokenStream {
        let name = &self.name;
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let consts = self.fields.iter().map(|f| {
            let const_name = &f.const_ident;
            let inner = f.inner_ty();
            quote! {
                pub const #const_name: u8 = <#inner as ::dope::manifold::Manifold>::ID;
            }
        });
        let uniqueness_const = self.uniqueness_const();
        quote! {
            impl #impl_generics #name #ty_generics #where_clause {
                #(#consts)*
                #uniqueness_const
            }
        }
    }

    fn uniqueness_const(&self) -> proc_macro2::TokenStream {
        if self.fields.len() < 2 {
            return quote! {};
        }
        let n = self.fields.len();
        let ids = self.fields.iter().map(|f| {
            let inner = f.inner_ty();
            quote! { <#inner as ::dope::manifold::Manifold>::ID }
        });
        quote! {
            #[doc(hidden)]
            pub const __MANIFOLD_ID_UNIQUE: () = {
                let __ids: [u8; #n] = [ #(#ids),* ];
                let mut __i = 0;
                while __i < __ids.len() {
                    let mut __j = __i + 1;
                    while __j < __ids.len() {
                        if __ids[__i] == __ids[__j] {
                            ::core::panic!(
                                "Dispatcher: duplicate Manifold::ID detected across fields"
                            );
                        }
                        __j += 1;
                    }
                    __i += 1;
                }
            };
        }
    }

    fn manifold_impl(&self) -> proc_macro2::TokenStream {
        let name = &self.name;
        let (_, ty_generics, where_clause) = self.generics.split_for_impl();
        let (brand, fresh) = self.generics.brand_lifetime();
        let impl_generics = {
            let params = self.generics.params.iter();
            quote! { <#fresh #(#params),*> }
        };
        let dispatch_arms = self.dispatch_arms(&brand);
        let activate_arms = self.activate_arms(&brand);
        let tick_calls = self.tick_calls();
        let post_coordinate_tick_calls = if self.coordinate {
            self.tick_calls()
        } else {
            Vec::new()
        };
        let shutdown_calls = self.shutdown_calls();
        let finish_calls = self.finish_calls();
        let idle_expr = self.idle_expr();
        let uniqueness_use = if self.fields.len() >= 2 {
            quote! { let _: () = Self::__MANIFOLD_ID_UNIQUE; }
        } else {
            quote! {}
        };
        let coordinate_tail = if self.coordinate {
            quote! {
                <#name #ty_generics>::coordinate(self.as_mut(), __driver);
            }
        } else {
            quote! {}
        };
        quote! {
            impl #impl_generics ::dope::runtime::dispatcher::Dispatcher<#brand> for #name #ty_generics #where_clause {
                fn dispatch(
                    mut self: ::core::pin::Pin<&mut Self>,
                    __ev: ::dope::Event<#brand>,
                    __driver: &mut ::dope::DriverContext<'_, #brand>,
                ) {
                    #uniqueness_use
                    let __route = __ev.route();
                    match __route {
                        #(#dispatch_arms)*
                        _ => {}
                    }
                }
                fn activate(
                    mut self: ::core::pin::Pin<&mut Self>,
                    __target: ::dope::driver::token::Token,
                    __driver: &mut ::dope::DriverContext<'_, #brand>,
                ) {
                    let __route = __target.route();
                    match __route {
                        #(#activate_arms)*
                        _ => {}
                    }
                }
                fn pre_park(
                    mut self: ::core::pin::Pin<&mut Self>,
                    __driver: &mut ::dope::DriverContext<'_, #brand>,
                ) {
                    #(#tick_calls)*
                    #coordinate_tail
                    #(#post_coordinate_tick_calls)*
                }
                fn idle(
                    self: ::core::pin::Pin<&Self>,
                    __region: &::dope::runtime::__private::RegionToken<#brand>,
                ) -> ::dope::runtime::dispatcher::Idle {
                    #idle_expr
                }
                fn shutdown(
                    mut self: ::core::pin::Pin<&mut Self>,
                    __driver: &mut ::dope::DriverContext<'_, #brand>,
                ) {
                    #(#shutdown_calls)*
                }
                fn finish(
                    mut self: ::core::pin::Pin<&mut Self>,
                    __context: &mut ::dope::runtime::dispatcher::FinishContext<'_, #brand>,
                ) {
                    #(#finish_calls)*
                }
            }
        }
    }

    fn dispatch_arms(&self, brand: &proc_macro2::TokenStream) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                let const_name = &f.const_ident;
                let inner = f.inner_ty();
                let body = f.wrap_body(|recv| {
                    quote! {
                        let _ = <#inner as ::dope::manifold::Manifold<#brand>>::ID;
                        ::dope::manifold::Manifold::dispatch(#recv, __ev, __driver);
                    }
                });
                quote! { __candidate if __candidate == Self::#const_name => { #body } }
            })
            .collect()
    }

    fn activate_arms(&self, brand: &proc_macro2::TokenStream) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                let const_name = &f.const_ident;
                let inner = f.inner_ty();
                let body = f.wrap_body(|recv| {
                    quote! {
                        let __typed =
                            ::dope::manifold::typed::TypedToken::<#inner>::try_new::<#brand>(__target)
                                .expect("dispatcher selected the token route");
                        ::dope::manifold::Manifold::activate(#recv, __typed, __driver);
                    }
                });
                quote! { __candidate if __candidate == Self::#const_name => { #body } }
            })
            .collect()
    }

    fn shutdown_calls(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                f.wrap_body(|recv| {
                    quote! {
                        ::dope::manifold::Manifold::shutdown(#recv, __driver);
                    }
                })
            })
            .collect()
    }

    fn finish_calls(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                f.wrap_body(|recv| {
                    quote! {
                        ::dope::manifold::Manifold::finish(#recv, __context);
                    }
                })
            })
            .collect()
    }

    fn tick_calls(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                f.wrap_body(|recv| {
                    quote! {
                        ::dope::manifold::Manifold::pre_park(#recv, __driver);
                    }
                })
            })
            .collect()
    }

    fn idle_expr(&self) -> proc_macro2::TokenStream {
        if self.fields.is_empty() {
            return quote! { ::dope::runtime::dispatcher::Idle::Park(::core::option::Option::None) };
        }
        let arms = self.fields.iter().map(|f| {
            f.wrap_body_ref(|recv| {
                quote! {
                    match ::dope::manifold::Manifold::idle(#recv, __region) {
                        ::dope::runtime::dispatcher::Idle::Busy => return ::dope::runtime::dispatcher::Idle::Busy,
                        __park => __acc = __acc.reduce(__park),
                    }
                }
            })
        });
        quote! {
            {
                let mut __acc = ::dope::runtime::dispatcher::Idle::Park(::core::option::Option::None);
                #(#arms)*
                __acc
            }
        }
    }

    fn projections_impl(&self) -> proc_macro2::TokenStream {
        let name = &self.name;
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let methods = self
            .fields
            .iter()
            .filter(|field| field.pinned)
            .map(|field| {
                let ty = &field.ty;
                let field = &field.name;
                let pin_name = format_ident!("{}_pin", field);
                let ref_name = format_ident!("{}_ref", field);
                quote! {
                    #[doc(hidden)]
                    pub fn #pin_name(
                        self: ::core::pin::Pin<&mut Self>,
                    ) -> ::core::pin::Pin<&mut #ty> {
                        self.project().#field
                    }

                    #[doc(hidden)]
                    pub fn #ref_name(
                        self: ::core::pin::Pin<&Self>,
                    ) -> ::core::pin::Pin<&#ty> {
                        self.project_ref().#field
                    }
                }
            });
        quote! {
            impl #impl_generics #name #ty_generics #where_clause {
                #(#methods)*
            }
        }
    }
}
