mod coordinate;
mod marker;
mod parser;
mod paths;
mod projection;
mod role;
mod routes;
mod state;

use crate::{brand, field};

type ParseError = syn::Error;

pub(crate) struct Application {
    name: syn::Ident,
    generics: syn::Generics,
    fields: Vec<field::Field>,
    states: Vec<state::State>,
    brand: brand::Brand,
    coordinate: bool,
    paths: paths::Paths,
}

impl Application {
    pub(crate) fn expand(input: syn::DeriveInput) -> proc_macro::TokenStream {
        let spec = match parser::Parser::new(input).parse() {
            Ok(spec) => spec,
            Err(error) => return error.to_compile_error().into(),
        };
        let route_consts = routes::Routes::new(&spec).generate();
        let coordinate_projection = coordinate::Coordinate::new(&spec).projection();
        let manifold_impl = spec.manifold_impl();
        let projections_impl = projection::Projection::new(&spec).generate();
        quote::quote! {
            #route_consts
            #coordinate_projection
            #manifold_impl
            #projections_impl
        }
        .into()
    }

    fn manifold_impl(&self) -> proc_macro2::TokenStream {
        let name = &self.name;
        let manifold = &self.paths.manifold;
        let runtime = &self.paths.runtime;
        let core = &self.paths.core;
        let (_, ty_generics, _) = self.generics.split_for_impl();
        let lifetime = &self.brand.lifetime;
        let binder = &self.brand.binder;
        let mut bounded_generics = self.generics.clone();
        for state in &self.states {
            let ty = &state.ty;
            if state.schedule {
                bounded_generics
                    .make_where_clause()
                    .predicates
                    .push(syn::parse_quote! { #ty: #manifold::timing::Schedule });
            }
        }
        for field in &self.fields {
            let inner = field.manifold_ty();
            bounded_generics
                .make_where_clause()
                .predicates
                .push(syn::parse_quote! { #inner: #manifold::dispatch::raw::Manifold<#lifetime> });
        }
        let (_, _, where_clause) = bounded_generics.split_for_impl();
        let impl_generics = {
            let params = self.generics.params.iter();
            quote::quote! { <#binder #(#params),*> }
        };
        let dispatch_arms = self.dispatch_arms(lifetime);
        let activate_arms = self.activate_arms(lifetime);
        let tick_calls = self.tick_calls();
        let install_calls = self.install_calls();
        let shutdown_calls = self.shutdown_calls();
        let finish_calls = self.finish_calls();
        let install_body = if install_calls.is_empty() {
            quote::quote! {}
        } else {
            quote::quote! {
                let mut __context = __install.context();
                #(#install_calls)*
            }
        };
        let shutdown_body = if shutdown_calls.is_empty() {
            quote::quote! {}
        } else {
            quote::quote! {
                let mut __driver = #manifold::dispatch::raw::Context::<#manifold::dispatch::raw::Retained>::new(
                    __shutdown.driver().reborrow(),
                );
                #(#shutdown_calls)*
            }
        };
        let finish_body = if finish_calls.is_empty() {
            quote::quote! {}
        } else {
            quote::quote! {
                let mut __context = __finish.context();
                #(#finish_calls)*
            }
        };
        let progress_expr = self.progress_expr(false);
        let shutdown_progress_expr = self.progress_expr(true);
        let has_route_consts = self.generics.type_params().next().is_none()
            || self.generics.lifetimes().next().is_some();
        let uniqueness_use = if self.fields.len() >= 2 && has_route_consts {
            quote::quote! { let _: () = Self::__MANIFOLD_ID_UNIQUE; }
        } else {
            quote::quote! {}
        };
        let coordinate_tail = if self.coordinate {
            coordinate::Coordinate::new(self).call()
        } else {
            quote::quote! {}
        };
        let field_context = if self.fields.is_empty() {
            quote::quote! {}
        } else {
            quote::quote! {
                let mut __driver = #manifold::dispatch::raw::Context::<#manifold::dispatch::raw::Retained>::new(
                    __driver.reborrow(),
                );
            }
        };
        let pre_park_context = if self.fields.is_empty() && !self.coordinate {
            quote::quote! {}
        } else {
            quote::quote! {
                let mut __driver = #manifold::dispatch::raw::Context::<#manifold::dispatch::raw::Retained>::new(
                    __driver.reborrow(),
                );
            }
        };
        quote::quote! {
            impl #impl_generics #runtime::executor::Application<#lifetime> for #name #ty_generics #where_clause {
                fn install<'__app>(
                    __call: #runtime::executor::raw::Install<'_, '__app, #lifetime, Self>,
                )
                where
                    #lifetime: '__app,
                {
                    // SAFETY: the derive visits every structurally pinned
                    // Manifold field in every lifecycle phase.
                    let (mut __app, mut __install) = unsafe {
                        __call.into_parts_unchecked()
                    };
                    #install_body
                }
                fn dispatch<'__driver, '__turn, '__app>(
                    __call: #runtime::executor::raw::Dispatch<
                        '_, '__driver, '__turn, '__app, #lifetime, Self,
                    >,
                ) -> ::core::ops::ControlFlow<#core::io::Event<#lifetime>>
                where
                    #lifetime: '__app,
                {
                    // SAFETY: this call owns the exact installed application;
                    // every routed owner is structurally pinned beneath it.
                    let (mut __app, __ev, __turn, mut __driver) = unsafe {
                        __call.into_parts_unchecked()
                    };
                    #field_context
                    #uniqueness_use
                    let __route = __ev.route();
                    match __route {
                        #(#dispatch_arms)*
                        _ => ::core::ops::ControlFlow::Continue(())
                    }
                }
                fn activate<'__driver, '__turn, '__app>(
                    __call: #runtime::executor::raw::Activate<
                        '_, '__driver, '__turn, '__app, #lifetime, Self,
                    >,
                )
                where
                    #lifetime: '__app,
                {
                    // SAFETY: this call owns the exact installed application;
                    // every routed owner is structurally pinned beneath it.
                    let (mut __app, __target, __turn, mut __driver) = unsafe {
                        __call.into_parts_unchecked()
                    };
                    #field_context
                    let __route = __target.route();
                    match __route {
                        #(#activate_arms)*
                        _ => {}
                    }
                }
                fn pre_park<'__driver, '__turn, '__app>(
                    __call: #runtime::executor::raw::PrePark<
                        '_, '__driver, '__turn, '__app, #lifetime, Self,
                    >,
                )
                where
                    #lifetime: '__app,
                {
                    // SAFETY: this call owns the exact installed application;
                    // every driven owner is structurally pinned beneath it.
                    let (mut __app, __turn, mut __driver) = unsafe {
                        __call.into_parts_unchecked()
                    };
                    #pre_park_context
                    #coordinate_tail
                    #(#tick_calls)*
                }
                fn progress(
                    __call: #runtime::executor::raw::Progress<'_, '_, #lifetime, Self>,
                ) -> #core::driver::schedule::Progress<#lifetime> {
                    // SAFETY: the pin belongs to the exact installed application.
                    let (__app, __region) = unsafe { __call.into_parts_unchecked() };
                    #progress_expr
                }
                fn shutdown_progress(
                    __call: #runtime::executor::raw::Progress<'_, '_, #lifetime, Self>,
                ) -> #core::driver::schedule::Progress<#lifetime> {
                    // SAFETY: the pin belongs to the exact installed application.
                    let (__app, __region) = unsafe { __call.into_parts_unchecked() };
                    #shutdown_progress_expr
                }
                fn shutdown<'__driver, '__turn, '__app>(
                    __call: #runtime::executor::raw::Shutdown<
                        '_, '__driver, '__turn, '__app, #lifetime, Self,
                    >,
                ) -> #runtime::executor::raw::Pending<'__app, #lifetime, Self>
                where
                    #lifetime: '__app,
                {
                    // SAFETY: shutdown traverses every structurally pinned
                    // Manifold field before completing this exact-root proof.
                    let (mut __app, __turn, mut __shutdown) = unsafe {
                        __call.into_parts_unchecked()
                    };
                    #shutdown_body
                    __shutdown.pending()
                }
                fn finish<'__finalization, '__app>(
                    __call: #runtime::executor::raw::Finish<
                        '_, '__finalization, '__app, #lifetime, Self,
                    >,
                )
                where
                    #lifetime: '__app,
                {
                    // SAFETY: finish traverses every structurally pinned
                    // Manifold field under the exact post-quiescence proof.
                    let (mut __app, mut __finish) = unsafe {
                        __call.into_parts_unchecked()
                    };
                    #finish_body
                }
            }
        }
    }

    fn dispatch_arms(&self, brand: &proc_macro2::TokenStream) -> Vec<proc_macro2::TokenStream> {
        let manifold = &self.paths.manifold;
        let runtime = &self.paths.runtime;
        self.fields
            .iter()
            .map(|f| {
                let inner = f.manifold_ty();
                let body = f.wrap_body(runtime, |recv| {
                    quote::quote! {
                        let _ = <#inner as #manifold::dispatch::raw::Manifold<#brand>>::ID;
                        let mut __context = __driver.narrow::<<#inner as
                            #manifold::dispatch::raw::Manifold<#brand>>::Dispatch>();
                        // SAFETY: derived Application::dispatch requires the exact
                        // pinned root to have been installed by the runtime.
                        match unsafe { #manifold::dispatch::raw::Manifold::dispatch(#recv, __ev, __turn.reborrow(), &mut __context) } {
                            ::core::ops::ControlFlow::Continue(()) => {}
                            ::core::ops::ControlFlow::Break(__ev) => {
                                return ::core::ops::ControlFlow::Break(__ev);
                            }
                        }
                    }
                });
                quote::quote! {
                    __candidate if __candidate == <#inner as #manifold::dispatch::raw::Manifold<#brand>>::ID => {
                        #body
                        ::core::ops::ControlFlow::Continue(())
                    }
                }
            })
            .collect()
    }

    fn activate_arms(&self, brand: &proc_macro2::TokenStream) -> Vec<proc_macro2::TokenStream> {
        let manifold = &self.paths.manifold;
        let runtime = &self.paths.runtime;
        self.fields
            .iter()
            .map(|f| {
                let inner = f.manifold_ty();
                let route = quote::quote! {
                    <#inner as #manifold::dispatch::raw::Manifold<#brand>>::ID
                };
                let body = f.wrap_body(runtime, |recv| {
                    quote::quote! {
                        // SAFETY: this arm is guarded by the exact Manifold ID.
                        let __typed = unsafe {
                            <#inner as #manifold::dispatch::raw::Manifold<#brand>>::token_from_route_unchecked(
                                __target,
                            )
                        };
                        let mut __context = __driver.narrow::<<#inner as
                            #manifold::dispatch::raw::Manifold<#brand>>::Activate>();
                        // SAFETY: derived Application::activate has the same
                        // installed-root precondition.
                        unsafe { #manifold::dispatch::raw::Manifold::activate(#recv, __typed, __turn.reborrow(), &mut __context) };
                    }
                });
                quote::quote! {
                    __candidate if __candidate == #route => {
                        #body
                    }
                }
            })
            .collect()
    }

    fn shutdown_calls(&self) -> Vec<proc_macro2::TokenStream> {
        let manifold = &self.paths.manifold;
        let runtime = &self.paths.runtime;
        let lifetime = &self.brand.lifetime;
        self.fields
            .iter()
            .map(|f| {
                let inner = f.manifold_ty();
                f.wrap_body(runtime, |recv| {
                    quote::quote! {
                        let mut __context = __driver.narrow::<<#inner as
                            #manifold::dispatch::raw::Manifold<#lifetime>>::Shutdown>();
                        #manifold::dispatch::raw::Manifold::shutdown(
                            #recv,
                            __turn.reborrow(),
                            &mut __context,
                        );
                    }
                })
            })
            .collect()
    }

    fn install_calls(&self) -> Vec<proc_macro2::TokenStream> {
        let manifold = &self.paths.manifold;
        let runtime = &self.paths.runtime;
        self.fields
            .iter()
            .map(|f| {
                f.wrap_body(runtime, |recv| {
                    quote::quote! {
                        #manifold::dispatch::raw::Manifold::install(
                            #recv,
                            &mut __context,
                        );
                    }
                })
            })
            .collect()
    }

    fn finish_calls(&self) -> Vec<proc_macro2::TokenStream> {
        let manifold = &self.paths.manifold;
        let runtime = &self.paths.runtime;
        self.fields
            .iter()
            .map(|f| {
                f.wrap_body(runtime, |recv| {
                    quote::quote! {
                        #manifold::dispatch::raw::Manifold::finish(
                            #recv,
                            &mut __context,
                        );
                    }
                })
            })
            .collect()
    }

    fn tick_calls(&self) -> Vec<proc_macro2::TokenStream> {
        let manifold = &self.paths.manifold;
        let runtime = &self.paths.runtime;
        let lifetime = &self.brand.lifetime;
        let count = self.fields.len();
        self.fields
            .iter()
            .enumerate()
            .map(|(index, f)| {
                let inner = f.manifold_ty();
                let participants = count - index;
                let body = f.wrap_body(runtime, |recv| {
                    quote::quote! {
                        let mut __context = __driver.narrow::<<#inner as
                            #manifold::dispatch::raw::Manifold<#lifetime>>::PrePark>();
                        // SAFETY: derived Application::pre_park has the same
                        // installed-root precondition.
                        unsafe { #manifold::dispatch::raw::Manifold::pre_park(#recv, __turn.reborrow(), &mut __context) };
                    }
                });
                quote::quote! {
                    let __share = __turn.reborrow().maintenance().share::<#participants>();
                    #body
                    ::core::mem::drop(__share);
                }
            })
            .collect()
    }

    fn progress_expr(&self, shutdown: bool) -> proc_macro2::TokenStream {
        let core = &self.paths.core;
        let manifold = &self.paths.manifold;
        let runtime = &self.paths.runtime;
        let method = if shutdown {
            quote::format_ident!("shutdown_progress")
        } else {
            quote::format_ident!("progress")
        };
        let schedules = if shutdown {
            Vec::new()
        } else {
            self.states
                .iter()
                .filter(|state| state.schedule)
                .map(|state| {
                    let name = &state.name;
                    quote::quote! {
                        if let ::core::option::Option::Some(__deadline) =
                            #manifold::timing::Schedule::deadline(
                                &__app.as_ref().get_ref().#name,
                            )
                        {
                            __acc = __acc.reduce(
                                #core::driver::schedule::Progress::until(__region, __deadline),
                            );
                        }
                    }
                })
                .collect::<Vec<_>>()
        };
        if self.fields.is_empty() && schedules.is_empty() {
            return quote::quote! {
                #core::driver::schedule::Progress::Quiescent
            };
        }
        let arms = self.fields.iter().map(|f| {
            f.wrap_body_ref(runtime, |recv| {
                quote::quote! {
                    match #manifold::dispatch::raw::Manifold::#method(#recv, __region) {
                        #core::driver::schedule::Progress::Runnable => {
                            return #core::driver::schedule::Progress::Runnable;
                        }
                        __progress => __acc = __acc.reduce(__progress),
                    }
                }
            })
        });
        quote::quote! {
            {
                let mut __acc = #core::driver::schedule::Progress::Quiescent;
                #(#arms)*
                #(#schedules)*
                __acc
            }
        }
    }
}
