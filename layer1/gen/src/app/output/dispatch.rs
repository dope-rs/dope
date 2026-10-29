use crate::app::{Owner, Spec, output::project::Project};

pub(crate) struct Dispatch<'a> {
    application: &'a Spec,
}

impl<'a> Dispatch<'a> {
    pub(crate) fn new(app: &'a Spec) -> Self {
        Self { application: app }
    }

    pub(crate) fn generate(&self, life: &syn::Lifetime) -> proc_macro2::TokenStream {
        let vdisp = self.application.brand.ident("__dispatch");
        let arms = self
            .application
            .owners
            .iter()
            .map(|owner| self.arm(owner, life));
        quote::quote! {
            #(#arms)*
            ::core::ops::ControlFlow::Break(#vdisp.into_event())
        }
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
                ::core::option::Option<<#inner as #exec::executor::raw::Layout<#life>>::ParkPlan>
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
        let [vchild, vplan, vweight] =
            ["__child", "__plan", "__weight"].map(|name| self.application.brand.ident(name));
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
                        if let ::core::option::Option::Some(#vchild) = &mut #vplan.#field {
                            #vweight = #parks::prepare_park(#recv, #vchild)?.get();
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
        let count = app.owners.len();
        let mut plan = self.cursor_count();
        app.owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                let peers = count - index;
                let body = self.tick(owner, exec, &syn::Index::from(plan));
                plan += usize::from(owner.is_app());
                if peers == 1 {
                    quote::quote! {{
                        let #vpark = #vpark.reborrow();
                        #body
                    }}
                } else {
                    let coord = index + usize::from(app.coordinate);
                    let rest = self
                        .application
                        .brand
                        .ident(&format!("__coord_{}", coord + 1));
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
                }
            })
            .collect()
    }

    fn arm(&self, owner: &Owner, life: &syn::Lifetime) -> proc_macro2::TokenStream {
        let [vapp, vcand, vdisp, vdriver, vevent, vplan, vturn] = [
            "__app",
            "__candidate",
            "__dispatch",
            "__driver",
            "__event",
            "__plan",
            "__turn",
        ]
        .map(|name| self.application.brand.ident(name));
        let mf = self.application.paths.manifold();
        let exec = self.application.paths.runtime();
        let inner = owner.inner();
        if owner.is_app() {
            let body = Project::new(owner, self.application).map(
                quote::quote! { ::core::ops::ControlFlow::Break(#vevent) },
                |recv| {
                    quote::quote! {
                        <#inner as #exec::executor::raw::App<#life>>::dispatch(
                            #exec::executor::raw::Dispatch::new(
                                #recv,
                                #vevent,
                                #vturn,
                                #vdriver,
                            ),
                        )
                    }
                },
            );
            quote::quote! {
                let #vdisp = match #vdisp.route() {
                #vcand if <#inner as #exec::executor::raw::App<#life>>::ROUTES.has_route(#vcand) => {
                    match #vdisp.map(
                        |mut #vapp, #vevent, #vturn, #vdriver| #body,
                    ) {
                        ::core::ops::ControlFlow::Continue(()) =>
                            return ::core::ops::ControlFlow::Continue(()),
                        ::core::ops::ControlFlow::Break(#vdisp) => #vdisp,
                    }
                }
                _ => #vdisp,
                };
            }
        } else {
            let dispatch = quote::quote! {
                <<#inner as #mf::app::dispatch::raw::Manifold<#life>>::Dispatch as
                    #mf::app::dispatch::event::Set<
                        #life,
                        #inner,
                        <#inner as #mf::app::dispatch::raw::Manifold<#life>>::Route,
                    >>
            };
            let body = Project::new(owner, self.application).map(
                quote::quote! { ::core::ops::ControlFlow::Break(#vevent) },
                |recv| {
                    quote::quote! {
                        let #vevent = #mf::app::dispatch::event::Call::new(
                            #vevent,
                            #vturn,
                            #vdriver,
                        );
                        #dispatch::call(
                                #recv,
                                #vplan,
                                #vevent,
                            )
                    }
                },
            );
            quote::quote! {
                let #vdisp = match (
                    #vdisp.route(),
                    #dispatch::select(),
                ) {
                (#vcand, ::core::option::Option::Some(#vplan)) if
                    #dispatch::ROUTES.has_route(#vcand) =>
                {
                    match #vdisp.map(
                        |mut #vapp, #vevent, #vturn, #vdriver| #body,
                    ) {
                        ::core::ops::ControlFlow::Continue(()) =>
                            return ::core::ops::ControlFlow::Continue(()),
                        ::core::ops::ControlFlow::Break(#vdisp) => #vdisp,
                    }
                }
                _ => #vdisp,
                };
            }
        }
    }

    fn tick(
        &self,
        owner: &Owner,
        exec: &proc_macro2::TokenStream,
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
        let life = app.brand.lifetime();
        let inner = owner.inner();
        Project::new(owner, self.application).map(quote::quote! {}, |recv| {
            if owner.is_app() {
                quote::quote! {
                    if let ::core::option::Option::Some(#vplan) =
                        &mut #vplan.#plan
                    {
                        /// SAFETY: preparation retained this selected child's plan,
                        /// and the projection and authority belong to the same installed root.
                        unsafe {
                            <#inner as #exec::executor::raw::App<#life>>::park(#recv, #vplan, #vpark)
                        }
                    }
                }
            } else {
                quote::quote! {
                    #vpark.map(|#vturn, #vdriver| {
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
                    });
                }
            }
        })
    }
}
