#![warn(unreachable_pub)]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Fields, FieldsNamed, Generics, Ident, ItemFn, Type, parse_macro_input};

#[proc_macro_derive(Forward, attributes(forward))]
pub fn forward(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let data = match &input.data {
        syn::Data::Struct(s) => s,
        _ => {
            return syn::Error::new_spanned(name, "Forward requires a struct")
                .to_compile_error()
                .into();
        }
    };
    let fields = match &data.fields {
        Fields::Named(f) => &f.named,
        _ => {
            return syn::Error::new_spanned(name, "Forward requires named fields")
                .to_compile_error()
                .into();
        }
    };
    let mut marked: Vec<&Ident> = Vec::new();
    for f in fields {
        if f.attrs.iter().any(|a| a.path().is_ident("forward")) {
            marked.push(f.ident.as_ref().expect("named field has ident"));
        }
    }
    let field = match marked.as_slice() {
        [one] => *one,
        [] => {
            return syn::Error::new_spanned(
                name,
                "Forward needs exactly one field marked `#[forward]`",
            )
            .to_compile_error()
            .into();
        }
        _ => {
            return syn::Error::new_spanned(name, "Forward accepts only one `#[forward]` field")
                .to_compile_error()
                .into();
        }
    };

    let field_ty: Option<&Type> = fields.iter().find_map(|f| {
        let ident = f.ident.as_ref()?;
        if ident == field { Some(&f.ty) } else { None }
    });
    let id_const = match field_ty {
        Some(ty) => quote! {
            const ID: u8 = <#ty as ::dope::manifold::Manifold>::ID;
        },
        None => quote! {},
    };
    let field_ty_tokens = match field_ty {
        Some(ty) => quote! { #ty },
        None => quote! { _ },
    };
    quote! {
        impl #impl_generics ::dope::manifold::Manifold for #name #ty_generics
        #where_clause
        {
            #id_const
            fn dispatch(
                self: ::core::pin::Pin<&mut Self>,
                ev: ::dope::Event,
                driver: &mut ::dope::Driver,
            ) {
                let _ = <#field_ty_tokens as ::dope::manifold::Manifold>::ID;
                ::dope::manifold::Manifold::dispatch(self.project().#field, ev, driver)
            }

            fn pre_park(self: ::core::pin::Pin<&mut Self>, driver: &mut ::dope::Driver) {
                ::dope::manifold::Manifold::pre_park(self.project().#field, driver)
            }
            fn idle(self: ::core::pin::Pin<&Self>) -> ::dope::Idle {
                ::dope::manifold::Manifold::idle(self.project_ref().#field)
            }
            fn on_wake(
                self: ::core::pin::Pin<&mut Self>,
                target: ::dope::manifold::route::TypedToken<Self>,
                driver: &mut ::dope::Driver,
            ) {
                // SAFETY: Forward propagates Manifold::ID, so wrapper's TypedToken<Self> bits match inner field's Manifold::ID.
                let __typed = unsafe { ::dope::manifold::route::TypedToken::<#field_ty_tokens>::from_raw_token(target.token()) };
                ::dope::manifold::Manifold::on_wake(self.project().#field, __typed, driver)
            }
        }
    }
    .into()
}

struct ManifoldField {
    name: Ident,
    ty: Type,
    optional: bool,
    const_ident: Ident,
}

impl ManifoldField {
    fn inner_ty(&self) -> proc_macro2::TokenStream {
        if self.optional {
            if let Type::Path(tp) = &self.ty
                && let Some(seg) = tp.path.segments.last()
                && seg.ident == "Option"
                && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
            {
                for a in &args.args {
                    if let syn::GenericArgument::Type(inner) = a {
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
        accessor_optional: proc_macro2::TokenStream,
        accessor_direct: proc_macro2::TokenStream,
        body_with: impl FnOnce(proc_macro2::TokenStream) -> proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let field = &self.name;
        if self.optional {
            let body = body_with(quote! { __inner });
            quote! {
                if let ::core::option::Option::Some(__inner) = __this.#field.#accessor_optional {
                    #body
                }
            }
        } else {
            body_with(quote! { __this.#field.#accessor_direct })
        }
    }
}

struct DispatcherSpec {
    name: Ident,
    generics: Generics,
    fields: Vec<ManifoldField>,
    coordinate: bool,
}

impl DispatcherSpec {
    fn parse(
        name: Ident,
        generics: Generics,
        named: &FieldsNamed,
        coordinate: bool,
    ) -> Result<Self, syn::Error> {
        let mut tagged: Vec<(Ident, Type, bool)> = Vec::new();
        let mut any_tagged = false;
        let mut all: Vec<(Ident, Type)> = Vec::new();
        for f in &named.named {
            let ident = f.ident.clone().expect("named field");
            let ty = f.ty.clone();
            all.push((ident.clone(), ty.clone()));
            let mut is_manifold = false;
            let mut optional = false;
            for attr in &f.attrs {
                if !attr.path().is_ident("manifold") {
                    continue;
                }
                is_manifold = true;
                if matches!(attr.meta, syn::Meta::Path(_)) {
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
                tagged.push((ident, ty, optional));
            }
        }

        let raw: Vec<(Ident, Type, bool)> = if any_tagged {
            tagged
        } else {
            all.into_iter().map(|(i, t)| (i, t, false)).collect()
        };

        let mut fields = Vec::with_capacity(raw.len());
        for (ident, ty, optional) in raw {
            let const_ident = format_ident!("{}_ROUTE", ident.to_string().to_uppercase());
            fields.push(ManifoldField {
                name: ident,
                ty,
                optional,
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
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let dispatch_arms = self.dispatch_arms();
        let wake_arms = self.wake_arms();
        let tick_calls = self.tick_calls();
        let shutdown_calls = self.shutdown_calls();
        let idle_expr = self.idle_expr();
        let uniqueness_use = if self.fields.len() >= 2 {
            quote! { let _: () = Self::__MANIFOLD_ID_UNIQUE; }
        } else {
            quote! {}
        };
        let coordinate_tail = if self.coordinate {
            quote! {
                <#name #ty_generics>::coordinate(self, __driver);
            }
        } else {
            quote! {}
        };
        quote! {
            impl #impl_generics ::dope::Dispatcher for #name #ty_generics #where_clause {
                fn dispatch(
                    self: ::core::pin::Pin<&mut Self>,
                    __ev: ::dope::Event,
                    __driver: &mut ::dope::Driver,
                ) {
                    #uniqueness_use
                    let mut __this = self.project();
                    let __route = __ev.route();
                    match __route {
                        #(#dispatch_arms)*
                        _ => {}
                    }
                }
                fn on_wake(
                    self: ::core::pin::Pin<&mut Self>,
                    __target: ::dope::runtime::token::Token,
                    __driver: &mut ::dope::Driver,
                ) {
                    let mut __this = self.project();
                    let __route = __target.route();
                    match __route {
                        #(#wake_arms)*
                        _ => {}
                    }
                }
                fn pre_park(mut self: ::core::pin::Pin<&mut Self>, __driver: &mut ::dope::Driver) {
                    {
                        let mut __this = self.as_mut().project();
                        #(#tick_calls)*
                    }
                    #coordinate_tail
                }
                fn idle(self: ::core::pin::Pin<&Self>) -> ::dope::Idle {
                    let __this = self.project_ref();
                    #idle_expr
                }
                fn on_shutdown(mut self: ::core::pin::Pin<&mut Self>, __driver: &mut ::dope::Driver) {
                    let mut __this = self.as_mut().project();
                    #(#shutdown_calls)*
                }
            }
        }
    }

    fn dispatch_arms(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                let const_name = &f.const_ident;
                let inner = f.inner_ty();
                let body = f.wrap_body(quote! { as_pin_mut() }, quote! { as_mut() }, |recv| {
                    quote! {
                        let _ = <#inner as ::dope::manifold::Manifold>::ID;
                        ::dope::manifold::Manifold::dispatch(#recv, __ev, __driver);
                    }
                });
                quote! { Self::#const_name => { #body } }
            })
            .collect()
    }

    fn wake_arms(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                let const_name = &f.const_ident;
                let inner = f.inner_ty();
                let body = f.wrap_body(
                    quote! { as_pin_mut() },
                    quote! { as_mut() },
                    |recv| {
                        quote! {
                            // SAFETY: gate verified __target.route() == <#inner as ::dope::manifold::Manifold>::ID.
                            let __typed = unsafe { ::dope::manifold::route::TypedToken::<#inner>::from_raw_token(__target) };
                            ::dope::manifold::Manifold::on_wake(#recv, __typed, __driver);
                        }
                    },
                );
                quote! { Self::#const_name => { #body } }
            })
            .collect()
    }

    fn shutdown_calls(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                f.wrap_body(quote! { as_pin_mut() }, quote! { as_mut() }, |recv| {
                    quote! {
                        ::dope::manifold::Manifold::on_shutdown(#recv, __driver);
                    }
                })
            })
            .collect()
    }

    fn tick_calls(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields
            .iter()
            .map(|f| {
                f.wrap_body(quote! { as_pin_mut() }, quote! { as_mut() }, |recv| {
                    quote! {
                        ::dope::manifold::Manifold::pre_park(#recv, __driver);
                    }
                })
            })
            .collect()
    }

    fn idle_expr(&self) -> proc_macro2::TokenStream {
        if self.fields.is_empty() {
            return quote! { ::dope::Idle::Park(::core::option::Option::None) };
        }
        let arms = self.fields.iter().map(|f| {
            f.wrap_body(quote! { as_pin_ref() }, quote! { as_ref() }, |recv| {
                quote! {
                    match ::dope::manifold::Manifold::idle(#recv) {
                        ::dope::Idle::Busy => return ::dope::Idle::Busy,
                        __park => __acc = __acc.reduce(__park),
                    }
                }
            })
        });
        quote! {
            {
                let mut __acc = ::dope::Idle::Park(::core::option::Option::None);
                #(#arms)*
                __acc
            }
        }
    }

    fn handles_impl(&self) -> proc_macro2::TokenStream {
        let name = &self.name;
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let handle_fns = self.fields.iter().map(|f| {
            let field = &f.name;
            let fn_name = format_ident!("{}_handle", field);
            if f.optional {
                let inner = f.inner_ty();
                quote! {
                    #[inline(always)]
                    pub fn #fn_name<'__d>(
                        self: ::core::pin::Pin<&mut Self>,
                    ) -> ::core::option::Option<::dope::fiber::Holding<'__d, #inner>>
                    where
                        Self: '__d,
                    {
                        // SAFETY: pinned Self's field address is stable for Self's pin scope; thread-per-core; the brand '__d is bounded by Self: '__d.
                        let __this = unsafe { ::core::pin::Pin::get_unchecked_mut(self) };
                        __this.#field.as_mut().map(|__m| {
                            let __ptr = ::core::ptr::NonNull::from(__m);
                            // SAFETY: same pin/brand argument as the non-optional arm; Some payload address is stable while pinned.
                            unsafe { ::dope::fiber::Holding::from_raw(__ptr) }
                        })
                    }
                }
            } else {
                let ty = &f.ty;
                quote! {
                    #[inline(always)]
                    pub fn #fn_name<'__d>(
                        self: ::core::pin::Pin<&mut Self>,
                    ) -> ::dope::fiber::Holding<'__d, #ty>
                    where
                        Self: '__d,
                    {
                        // SAFETY: pinned Self's field address is stable for Self's pin scope; thread-per-core; the brand '__d is bounded by Self: '__d.
                        let __this = unsafe { ::core::pin::Pin::get_unchecked_mut(self) };
                        let __ptr = ::core::ptr::NonNull::from(&mut __this.#field);
                        unsafe { ::dope::fiber::Holding::from_raw(__ptr) }
                    }
                }
            }
        });
        quote! {
            impl #impl_generics #name #ty_generics #where_clause {
                #(#handle_fns)*
            }
        }
    }
}

#[proc_macro_derive(Dispatcher, attributes(manifold, coordinate))]
pub fn dispatcher(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let generics = input.generics;
    let coordinate = input.attrs.iter().any(|a| a.path().is_ident("coordinate"));
    let data = match &input.data {
        syn::Data::Struct(s) => s,
        _ => {
            return syn::Error::new_spanned(&name, "Dispatcher requires a struct")
                .to_compile_error()
                .into();
        }
    };
    let named = match &data.fields {
        Fields::Named(n) => n,
        _ => {
            return syn::Error::new_spanned(&name, "Dispatcher requires named fields")
                .to_compile_error()
                .into();
        }
    };
    let spec = match DispatcherSpec::parse(name, generics, named, coordinate) {
        Ok(s) => s,
        Err(e) => return e.to_compile_error().into(),
    };
    let route_consts = spec.route_consts();
    let manifold_impl = spec.manifold_impl();
    let handles_impl = spec.handles_impl();
    quote! {
        #route_consts
        #manifold_impl
        #handles_impl
    }
    .into()
}

const HANDLER_DEFAULT_SIZE: usize = 256;

struct HandlerAttr {
    size: usize,
}

impl HandlerAttr {
    fn parse(attr: proc_macro2::TokenStream) -> Result<Self, syn::Error> {
        if attr.is_empty() {
            return Ok(Self {
                size: HANDLER_DEFAULT_SIZE,
            });
        }
        let meta: syn::Meta = syn::parse2(attr)?;
        let nv = match meta {
            syn::Meta::NameValue(nv) => nv,
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "#[handler] expects `size = N`",
                ));
            }
        };
        if !nv.path.is_ident("size") {
            return Err(syn::Error::new_spanned(
                nv.path,
                "#[handler] only supports `size = N`",
            ));
        }
        let lit = match nv.value {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(lit),
                ..
            }) => lit,
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "#[handler(size = ...)] expects integer literal",
                ));
            }
        };
        Ok(Self {
            size: lit.base10_parse::<usize>()?,
        })
    }
}

#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let size = match HandlerAttr::parse(attr.into()) {
        Ok(h) => h.size,
        Err(err) => return err.to_compile_error().into(),
    };

    let mut item_fn = parse_macro_input!(item as ItemFn);

    if item_fn.sig.asyncness.take().is_none() {
        return syn::Error::new_spanned(&item_fn.sig, "#[handler] requires `async fn`")
            .to_compile_error()
            .into();
    }

    let output_ty = match &item_fn.sig.output {
        syn::ReturnType::Default => quote! { () },
        syn::ReturnType::Type(_, ty) => quote! { #ty },
    };

    item_fn.sig.output = syn::parse_quote! {
        -> ::dope::o3::task::InlineFuture<'static, #output_ty, #size>
    };

    let original_block = item_fn.block.clone();
    let new_block: syn::Block = syn::parse_quote! {{
        ::dope::o3::task::InlineFuture::new(async move #original_block)
    }};
    *item_fn.block = new_block;

    quote! { #item_fn }.into()
}
