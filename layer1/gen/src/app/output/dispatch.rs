use proc_macro2::TokenStream;

use crate::app::{Owner, Spec, output::project::Project};

pub(crate) struct Dispatch<'a> {
    application: &'a Spec,
}

impl<'a> Dispatch<'a> {
    pub(crate) fn new(app: &'a Spec) -> Self {
        Self { application: app }
    }

    pub(crate) fn generate(&self, app_life: &syn::Lifetime) -> (TokenStream, TokenStream) {
        let app = self.application;
        let core = app.paths.core();
        let exec = app.paths.runtime();
        let life = app.brand.lifetime();
        let routes = quote::quote! { #core::driver::settings::Routes };
        let Some(last) = app.owners.last() else {
            return (
                TokenStream::new(),
                quote::quote! { const ROUTES: #routes = #routes::EMPTY; },
            );
        };
        let delegate = last.is_app() && !last.is_optional();
        let [
            vapp,
            vproject,
            vevent,
            vturn,
            vdriver,
            vnested,
            vneeds,
        ] = [
            "__app",
            "__project",
            "__event",
            "__turn",
            "__driver",
            "__NESTED",
            "__needs",
        ]
        .map(|name| app.brand.ident(name));
        let mut registered = quote::quote! { #routes::EMPTY };
        let mut observed = registered.clone();
        let mut admit = TokenStream::new();
        let mut members = TokenStream::new();
        let mut fields = Vec::with_capacity(app.owners.len());
        let [vset, vclipped] = ["__set", "__clipped"].map(|name| app.brand.ident(name));
        let miss = quote::quote! { ::core::ops::ControlFlow::Break(#vevent.into_dispatch()) };
        for (index, owner) in app.owners.iter().enumerate() {
            let inner = owner.inner();
            let field = owner.name();
            let project = Project::new(owner, app);
            let tail = delegate && index + 1 == app.owners.len();
            let (registration, set, scope, body) = if owner.is_app() {
                let child = quote::quote! { <#inner as #exec::executor::raw::App<#life>> };
                admit.extend(
                    project.shared(|recv| quote::quote! { #child::admit(#recv, #vneeds)?; }),
                );
                let body = if tail {
                    let project_tail = app.brand.ident("__dope_dispatch_tail");
                    members.extend(quote::quote! {
                        fn #project_tail(self: ::core::pin::Pin<&mut Self>) -> ::core::pin::Pin<&mut #inner> {
                            self.project().#field
                        }
                    });
                    quote::quote! {
                        #child::dispatch_from::<true>(
                            #exec::executor::raw::project_field(#vproject, Self::#project_tail),
                            #vevent, #vturn, #vdriver,
                        )
                    }
                } else {
                    project.map(miss.clone(), |recv| {
                        quote::quote! {
                            #child::dispatch(#recv, #vevent, #vturn.reborrow(), #vdriver.reborrow())
                        }
                    })
                };
                let registration = quote::quote! { #child::ROUTES };
                let set = quote::quote! { #registration.intersection(#child::DISPATCH_ROUTES) };
                (registration, set, quote::quote! { #core::driver::route::Route }, body)
            } else {
                let mf = app.paths.manifold();
                let manifold =
                    quote::quote! { <#inner as #mf::app::dispatch::raw::Manifold<#life>> };
                let registration = quote::quote! { #routes::singleton_id(#manifold::ID) };
                let include = quote::quote! {
                    *#vneeds = #vneeds.include(#core::driver::settings::claim::Claim::new(#registration))?;
                };
                admit.extend(if owner.is_optional() {
                    quote::quote! { if #vapp.project_ref().#field.is_some() { #include } }
                } else {
                    include
                });
                let dispatch = quote::quote! {
                    <#manifold::Dispatch as #mf::app::dispatch::event::Set<#life, #inner, #manifold::Route>>
                };
                let body = project.map(miss.clone(), |recv| {
                    quote::quote! {
                        #dispatch::call(#recv, #vevent, #vturn.reborrow(), #vdriver.reborrow())
                    }
                });
                (registration, quote::quote! { #dispatch::ROUTES }, quote::quote! { #dispatch::Scope }, body)
            };
            registered.extend(quote::quote! { .join(#registration) });
            if tail {
                observed.extend(quote::quote! { .join(#set) });
                fields.push((set, scope, body));
            } else {
                let routes_for_field = app.brand.ident(&format!("__DOPE_DISPATCH_{index}"));
                members.extend(quote::quote! {
                    const #routes_for_field: [(#routes, ::core::option::Option<#core::driver::settings::RouteSpan>); 2] = {
                        let #vset = #set;
                        let #vclipped = #vset.intersection(<Self as #exec::executor::raw::App<#life>>::ROUTES);
                        [(#vset, #vset.span()), (#vclipped, #vclipped.span())]
                    };
                });
                observed.extend(quote::quote! { .join(Self::#routes_for_field[0].0) });
                fields.push((quote::quote! { Self::#routes_for_field }, scope, body));
            }
        }
        let select = self.select(&fields, delegate);
        let event = app.brand.fresh("__event");
        let slot = quote::quote! { #exec::executor::raw::Dispatch<#event, #life> };
        let input = quote::quote! {
            #vevent: #slot,
            #vturn: #core::driver::schedule::Turn<'_, #life>,
            #vdriver: #core::driver::Context<'_, #life>,
        };
        let output = quote::quote! { ::core::ops::ControlFlow<#slot> };
        let dispatch = quote::quote! {
            const ROUTES: #routes = #registered;
            const DISPATCH_ROUTES: #routes = #observed;

            fn admit(
                #vapp: ::core::pin::Pin<&Self>,
                #vneeds: &mut #core::driver::settings::claim::Needs,
            ) -> ::core::result::Result<(), #core::driver::settings::errors::Claim> {
                #admit
                ::core::result::Result::Ok(())
            }

            fn dispatch<#event>(#vapp: ::core::pin::Pin<&mut Self>, #input) -> #output {
                <Self as #exec::executor::raw::App<#life>>::dispatch_from::<false>(
                    || #vapp, #vevent, #vturn, #vdriver,
                )
            }

            fn dispatch_from<#app_life, #event, const #vnested: ::core::primitive::bool>(
                #vproject: impl ::core::ops::FnOnce() -> ::core::pin::Pin<&#app_life mut Self>,
                #input
            ) -> #output
            where
                Self: #app_life,
            {
                #select
            }
        };
        (members, dispatch)
    }

    fn select(&self, fields: &[(TokenStream, TokenStream, TokenStream)], delegate: bool) -> TokenStream {
        let [
            vapp,
            vdriver,
            vevent,
            vroute,
            vset,
            vspan,
            vproject,
            vnested,
        ] = [
            "__app",
            "__driver",
            "__event",
            "__route",
            "__set",
            "__span",
            "__project",
            "__NESTED",
        ]
        .map(|name| self.application.brand.ident(name));
        let flow = quote::quote! { ::core::ops::ControlFlow };
        let miss = quote::quote! { #flow::Break(#vevent) };
        let (guarded, fallback) = match fields.split_last() {
            Some(((_, _, tail), prefix)) if delegate => (prefix, tail),
            _ => (fields, &miss),
        };
        let Some(((first_set, first_scope, first), rest)) = guarded.split_first() else {
            return fallback.clone();
        };
        let start = quote::quote! {
            let mut #vdriver = #vdriver;
        };
        let select_event = |set: &TokenStream, selected: &TokenStream, fresh: bool| {
            let scope = if fresh {
                quote::quote! { #vnested as ::core::primitive::usize }
            } else {
                quote::quote! { 0 }
            };
            quote::quote! {{
                let #vset = &#set[#scope];
                #vevent.within::<#selected>(|#vroute| match #vset {
                    (_, ::core::option::Option::Some(#vspan)) => #vspan.has_route(#vroute),
                    (#vset, ::core::option::Option::None) => #vset.has_route(#vroute),
                })
            }}
        };
        let returned = |body: &TokenStream| {
            quote::quote! {
                match #body {
                    #flow::Continue(()) => return #flow::Continue(()),
                    #flow::Break(#vevent) => #vevent,
                }
            }
        };
        let finish = if delegate {
            quote::quote! {
                let #vproject = || #vapp;
                #fallback
            }
        } else {
            miss.clone()
        };
        if rest.is_empty() && !delegate {
            let select = select_event(first_set, first_scope, true);
            return quote::quote! {
                #start
                match #select {
                    ::core::result::Result::Ok(#vevent) => {
                        let mut #vapp = #vproject();
                        #first
                    }
                    ::core::result::Result::Err(#vevent) => #miss,
                }
            };
        }
        let entries: Vec<_> = (0..guarded.len())
            .map(|i| self.application.brand.fresh(&format!("__dispatch_{i}")))
            .collect();
        let select = entries.iter().zip(guarded).map(|(entry, (set, scope, _))| {
            let select = select_event(set, scope, true);
            quote::quote! {
                let #vevent = match #select {
                    ::core::result::Result::Ok(#vevent) => {
                        #vapp = #vproject();
                        break #entry #vevent;
                    }
                    ::core::result::Result::Err(#vevent) => #vevent,
                };
            }
        });
        let first_entry = &entries[0];
        let first_return = returned(first);
        let mut path = quote::quote! {
            let #vevent = #first_entry: {
                #(#select)*
                return #fallback;
            };
            let #vevent = #first_return;
        };
        // Each entry receives the original event directly. A returned event falls
        // through the remaining fields with their original declaration order.
        for (index, (entry, (set, scope, body))) in entries.iter().zip(guarded).enumerate().skip(1) {
            let next = self.application.brand.fresh(&format!("__next_{index}"));
            let select = select_event(set, scope, false);
            let deliver = if !delegate && index + 1 == guarded.len() {
                quote::quote! { return #body }
            } else {
                returned(body)
            };
            path = quote::quote! {
                let #vevent = #next: {
                    let #vevent = #entry: {
                        #path
                        match #select {
                            ::core::result::Result::Ok(#vevent) => break #entry #vevent,
                            ::core::result::Result::Err(#vevent) => break #next #vevent,
                        }
                    };
                    #deliver
                };
            };
        }
        quote::quote! { #start let mut #vapp; #path #finish }
    }

    pub(crate) fn wakes(
        &self,
        life: &syn::Lifetime,
    ) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
        let [vact, vcall, vplan, vroute] = ["__activate", "__call", "__plan", "__route"]
            .map(|name| self.application.brand.ident(name));
        let app = self.application;
        let core = app.paths.core();
        let mf = app.paths.manifold();
        let exec = app.paths.runtime();
        let mut calls = proc_macro2::TokenStream::new();
        let mut joins = proc_macro2::TokenStream::new();
        for owner in &app.owners {
            let inner = owner.inner();
            let view = Project::new(owner, app);
            let (routes, call) = if owner.is_app() {
                let child = quote::quote! { <#inner as #exec::executor::raw::App<#life>> };
                let call = view.map(quote::quote! { false }, |recv| {
                    quote::quote! {
                        /// SAFETY: the selected route, pinned child, and activation
                        /// operation belong to this exact installed root.
                        unsafe { #child::activate(#recv, #vact.reborrow()) }
                    }
                });
                (quote::quote! { #child::WAKE_ROUTES }, call)
            } else {
                let wakes = quote::quote! {
                    <<#inner as #mf::app::dispatch::raw::Manifold<#life>>::Activate as
                        #mf::app::dispatch::raw::Wakes<
                            #life,
                            #inner,
                            <#inner as #mf::app::dispatch::raw::Manifold<#life>>::Route,
                        >>
                };
                let call = view.map(quote::quote! { false }, |recv| {
                    quote::quote! {
                        let #vcall = #exec::executor::raw::Call::new(
                            #recv,
                            #vplan,
                            #vact.reborrow(),
                        );
                        /// SAFETY: `__plan` came from `select` on a route present
                        /// in `ROUTES`, and the call retains the same pinned root
                        /// and activation operation.
                        unsafe { #wakes::call(#vcall) };
                        true
                    }
                });
                (
                    quote::quote! { #wakes::ROUTES },
                    quote::quote! {{
                        if let ::core::option::Option::Some(#vplan) = #wakes::select() {
                            #call
                        } else {
                            false
                        }
                    }},
                )
            };
            calls.extend(quote::quote! {
                if #routes.has_route(#vroute) && #call {
                    return true;
                }
            });
            joins.extend(quote::quote! { .join(#routes) });
        }
        (
            calls,
            quote::quote! { #core::driver::settings::Routes::EMPTY #joins },
        )
    }

    pub(crate) fn park_plan(
        &self,
    ) -> (
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
    ) {
        let [vpark, vplan] = ["__park", "__plan"].map(|name| self.application.brand.ident(name));
        let app = self.application;
        let exec = app.paths.runtime();
        let life = app.brand.lifetime();
        let mut types = Vec::new();
        let mut values = Vec::new();
        let mut cursors = proc_macro2::TokenStream::new();
        for (name, count) in self.cursors() {
            if count > 1 {
                let field = syn::Index::from(types.len());
                let method = quote::format_ident!("{name}_cursor");
                let local = self.application.brand.ident(&format!("__{name}_cursor"));
                types.push(quote::quote! { #exec::executor::raw::Cursor<#count> });
                values.push(quote::quote! { #exec::executor::raw::Cursor::INITIAL });
                let mut start = quote::quote! { #vpark.#method(&mut #vplan.#field) };
                if name == "coord" {
                    start = quote::quote! {
                        if <Self as #exec::executor::raw::App<#life>>::COORDS <= 1 {
                            #exec::executor::raw::Cursor::<#count>::INITIAL
                        } else {
                            #start
                        }
                    };
                }
                cursors.extend(quote::quote! {
                    let #local = #start;
                });
            }
        }
        for owner in app.owners.iter().filter(|owner| owner.is_app()) {
            let inner = owner.inner();
            types.push(quote::quote! {
                <#inner as #exec::executor::raw::Layout<#life>>::ParkPlan
            });
            values.push(quote::quote! {
                <#inner as #exec::executor::raw::App<#life>>::select_park()
            });
        }
        if self.cache().is_some() {
            let count = app.owners.len() + usize::from(app.coordinate);
            types.push(quote::quote! { #exec::executor::raw::Weights<#count> });
            values.push(quote::quote! { #exec::executor::raw::Weights::ZERO });
        }
        (
            quote::quote! { (#(#types,)*) },
            quote::quote! { (#(#values,)*) },
            cursors,
        )
    }

    fn cursors(&self) -> [(&str, usize); 3] {
        let app = self.application;
        let count = app.owners.len();
        [
            ("app", count),
            ("maint", count),
            ("coord", count + usize::from(app.coordinate)),
        ]
    }

    fn cursor_count(&self) -> usize {
        self.cursors()
            .iter()
            .filter(|(_, count)| *count > 1)
            .count()
    }

    fn cache(&self) -> Option<syn::Index> {
        let app = self.application;
        if app.owners.len() + usize::from(app.coordinate) <= 1
            || !app
                .owners
                .iter()
                .any(|owner| owner.is_optional() || owner.is_app())
        {
            return None;
        }
        let cursors = self.cursor_count();
        let children = app.owners.iter().filter(|owner| owner.is_app()).count();
        Some(syn::Index::from(cursors + children))
    }

    pub(crate) fn coords(&self) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
        let [vplan, vweights] =
            ["__plan", "__weights"].map(|name| self.application.brand.ident(name));
        let exec = self.application.paths.runtime();
        let weights = self.weights();
        let upper = weights.iter().map(|(upper, _)| upper);
        let live = weights.iter().map(|(_, live)| live);
        let measure = match self.cache() {
            Some(field) => quote::quote! {
                let #vweights = [#(#live,)*];
                #vplan.#field.prepare(#vweights)
            },
            None => quote::quote! {
                #exec::executor::raw::Count::sum([#(#live,)*])
            },
        };
        (
            quote::quote! { 0usize #(.saturating_add(#upper))* },
            measure,
        )
    }

    pub(crate) fn shares(&self) -> proc_macro2::TokenStream {
        let [vpark, vplan, vweights] =
            ["__park", "__plan", "__weights"].map(|name| self.application.brand.ident(name));
        let weights = self.weights();
        if weights.len() <= 1 {
            return quote::quote! {};
        }
        let app = self.application;
        let exec = app.paths.runtime();
        let life = app.brand.lifetime();
        let upper = weights.iter().map(|(upper, _)| upper);
        let values = match self.cache() {
            Some(field) => quote::quote! {
                if <Self as #exec::executor::raw::App<#life>>::COORDS
                    <= #vpark.coord_left()
                {
                    [#(#upper,)*]
                } else {
                    #vplan.#field.values()
                }
            },
            None => quote::quote! { [#(#upper,)*] },
        };
        let mut suffix = proc_macro2::TokenStream::new();
        let mut next = quote::quote! { 0usize };
        for index in (1..weights.len()).rev() {
            let local = self.application.brand.ident(&format!("__coord_{index}"));
            suffix.extend(quote::quote! {
                let #local = #vweights[#index] + #next;
            });
            next = quote::quote! { #local };
        }
        quote::quote! {
            let #vweights = #values;
            #suffix
        }
    }

    fn weights(&self) -> Vec<(proc_macro2::TokenStream, proc_macro2::TokenStream)> {
        let [vplan, vweight] =
            ["__plan", "__weight"].map(|name| self.application.brand.ident(name));
        let app = self.application;
        let exec = app.paths.runtime();
        let mf = app.paths.manifold();
        let life = app.brand.lifetime();
        let mut weights = Vec::new();
        if app.coordinate {
            weights.push((quote::quote! { 1usize }, quote::quote! { 1usize }));
        }
        let mut plan = self.cursor_count();
        for owner in &app.owners {
            let inner = owner.inner();
            let (upper, visit) = if owner.is_app() {
                let parks = quote::quote! {
                    <#inner as #exec::executor::raw::App<#life>>
                };
                let field = syn::Index::from(plan);
                plan += 1;
                let visit = Project::new(owner, self.application).shared(|recv| {
                    quote::quote! {
                        if #parks::PARK {
                            #vweight = #parks::prepare_park(#recv, &mut #vplan.#field)?.get();
                        }
                    }
                });
                (quote::quote! { #parks::COORDS }, visit)
            } else {
                let manifold = quote::quote! {
                    <#inner as #mf::app::dispatch::raw::Manifold<#life>>
                };
                let visit = Project::new(owner, self.application).shared(|_| {
                    quote::quote! { #vweight = #manifold::COORDS; }
                });
                (quote::quote! { #manifold::COORDS }, visit)
            };
            weights.push((
                upper,
                quote::quote! {{
                    let mut #vweight = 0usize;
                    #visit
                    #vweight
                }},
            ));
        }
        weights
    }

    pub(crate) fn ticks(&self) -> Vec<proc_macro2::TokenStream> {
        let [vappcur, vcoord, vmaint, vpark, vweights] = [
            "__app_cursor",
            "__coord_cursor",
            "__maint_cursor",
            "__park",
            "__weights",
        ]
        .map(|name| self.application.brand.ident(name));
        let app = self.application;
        let exec = app.paths.runtime();
        let life = app.brand.lifetime();
        let count = app.owners.len();
        let mut plan = self.cursor_count();
        app.owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                let peers = count - index;
                let field = syn::Index::from(plan);
                plan += usize::from(owner.is_app());
                let visit = Project::new(owner, app).map(quote::quote! {}, |recv| {
                    let body = self.tick(owner, recv, &field);
                    if peers == 1 {
                        return quote::quote! {{
                            let #vpark = #vpark.reborrow();
                            #body
                        }};
                    }
                    let coord = index + usize::from(app.coordinate);
                    let rest = app.brand.ident(&format!("__coord_{}", coord + 1));
                    quote::quote! {
                        #vpark.share::<#peers, _>(
                            [
                                #vappcur.rank::<#index>(),
                                #vmaint.rank::<#index>(),
                                #vcoord.rank::<#coord>(),
                            ],
                            [#vweights[#coord], #rest],
                            |#vpark| { #body },
                        );
                    }
                });
                if owner.is_app() {
                    let inner = owner.inner();
                    quote::quote! { if <#inner as #exec::executor::raw::App<#life>>::PARK { #visit } }
                } else {
                    visit
                }
            })
            .collect()
    }

    fn tick(
        &self,
        owner: &Owner,
        recv: proc_macro2::TokenStream,
        plan: &syn::Index,
    ) -> proc_macro2::TokenStream {
        let [vctx, vdriver, vpark, vplan, vroot, vturn] = [
            "__context",
            "__driver",
            "__park",
            "__plan",
            "__root",
            "__turn",
        ]
        .map(|name| self.application.brand.ident(name));
        let app = self.application;
        let mf = app.paths.manifold();
        let exec = app.paths.runtime();
        let life = app.brand.lifetime();
        let inner = owner.inner();
        if owner.is_app() {
            quote::quote! {
                /// SAFETY: preparation retained this selected child's plan,
                /// and the projection and authority belong to the same installed root.
                unsafe {
                    <#inner as #exec::executor::raw::App<#life>>::park(#recv, &mut #vplan.#plan, #vpark)
                }
            }
        } else {
            quote::quote! {
                let (#vturn, #vdriver) = #vpark.into_parts();
                let mut #vroot =
                    #mf::app::dispatch::raw::Context::<
                        #mf::app::dispatch::raw::Retained,
                    >::new(#vdriver);
                let mut #vctx = #vroot.narrow::<<#inner as
                    #mf::app::dispatch::raw::Manifold<#life>>::PrePark>();
                /// SAFETY: the enclosing dispatcher installed this
                /// pinned owner, and `__recv` projects that same field.
                unsafe {
                    #mf::app::dispatch::raw::Manifold::pre_park(
                        #recv,
                        #vturn,
                        &mut #vctx,
                    )
                };
            }
        }
    }
}
