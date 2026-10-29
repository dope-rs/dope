use crate::app::{
    Owner, Spec,
    output::{bounds, control, dispatch::Dispatch, progress, project::Project, routes},
};

pub(crate) fn generate(app: &Spec, ctl: &control::Control<'_>) -> proc_macro2::TokenStream {
    let [
        vact,
        vapp,
        vcall,
        vcoord1,
        vcoord,
        vdisp,
        vdriver,
        vfinish,
        vinstall,
        vpark,
        vplan,
        vprog,
        vroute,
        vstop,
        vturn,
    ] = [
        "__activate",
        "__app",
        "__call",
        "__coord_1",
        "__coord_cursor",
        "__dispatch",
        "__driver",
        "__finish",
        "__install",
        "__park",
        "__plan",
        "__progress",
        "__route",
        "__shutdown",
        "__turn",
    ]
    .map(|name| app.brand.ident(name));
    let name = &app.name;
    let core = app.paths.core();
    let exec = app.paths.runtime();
    let life = app.brand.lifetime();
    let args = app.generics.split_for_impl().1;
    let app_life = app.brand.fresh("__app");
    let driver = app.brand.fresh("__driver");
    let turn = app.brand.fresh("__turn");
    let root = app.brand.fresh("__root");
    let owned = app.brand.fresh("__owner");
    let end = app.brand.fresh("__finish");
    let epoch = app.brand.fresh("__epoch");
    let bound = app.brand.generics(&bounds::lifecycle(app));
    let (head, _, clause) = bound.split_for_impl();
    let layout = app.brand.generics(&bounds::layout(app));
    let (layout_head, _, layout_clause) = layout.split_for_impl();
    let router = Dispatch::new(app);
    let routes = router.generate(life);
    let (wcall, wake_set) = router.wakes(life);
    let (plan, select, cursors) = router.park_plan();
    let (coords, measure) = router.coords();
    let shares = router.shares();
    let ticks = router.ticks();
    let active = progress::generate(app, false);
    let stop = progress::generate(app, true);
    let set = routes::sets(app);
    let mut park = ctl.park();
    if app.coordinate && !app.owners.is_empty() {
        let peers = app.owners.len() + 1;
        park = quote::quote! {
            #vpark.share_coord::<#peers, _>(
                #vcoord.rank::<0>(),
                [1, #vcoord1],
                |mut #vpark| { #park },
            );
        };
    }
    let mut setup = proc_macro2::TokenStream::new();
    let mut shutdown = proc_macro2::TokenStream::new();
    let mut finish = proc_macro2::TokenStream::new();
    let mut released = proc_macro2::TokenStream::new();
    let vneeds = app.brand.ident("__needs");
    let mut admit = proc_macro2::TokenStream::new();
    for owner in &app.owners {
        let inner = owner.inner();
        let visit = if owner.is_app() {
            Project::new(owner, app).shared(|recv| {
                quote::quote! {
                    <#inner as #exec::executor::raw::App<#life>>::admit(#recv, #vneeds)?;
                }
            })
        } else {
            let manifold = app.paths.manifold();
            let include = quote::quote! {
                *#vneeds = #vneeds.include(#core::driver::settings::claim::Claim::new(
                    #core::driver::settings::Routes::singleton_id(
                        <#inner as #manifold::app::dispatch::raw::Manifold<#life>>::ID,
                    ),
                ))?;
            };
            if owner.is_optional() {
                let field = owner.name();
                quote::quote! {
                    if #vapp.project_ref().#field.is_some() { #include }
                }
            } else {
                include
            }
        };
        admit.extend(visit);
        let [install, stop, end] = lifecycle(app, owner);
        setup.extend(quote::quote! { #vinstall = #install; });
        shutdown.extend(quote::quote! { (#vturn, #vdriver) = #stop; });
        finish.extend(quote::quote! { (#vinstall, #vfinish) = #end; });
        if owner.is_app() {
            let field = owner.name();
            let visit = if owner.is_optional() {
                quote::quote! {
                    #vcall.optional(|#vapp| {
                        ::core::option::Option::as_pin_mut(#vapp.project().#field)
                    })
                }
            } else {
                quote::quote! { #vcall.visit(|#vapp| #vapp.project().#field) }
            };
            released.extend(quote::quote! {
                // SAFETY: this declared pinned owned field retains its exact
                // source and presence throughout this root's client scope.
                #vcall = unsafe { #visit };
            });
        }
    }

    let admit = if app.owners.is_empty() {
        proc_macro2::TokenStream::new()
    } else {
        quote::quote! {
            fn admit(
                #vapp: ::core::pin::Pin<&Self>,
                #vneeds: &mut #core::driver::settings::claim::Needs,
            ) -> ::core::result::Result<(), #core::driver::settings::errors::Claim> {
                #admit
                ::core::result::Result::Ok(())
            }
        }
    };

    // The live helper shields generated bindings from unseen outer const names.
    quote::quote! {
        const fn #vapp<const N: ::core::primitive::usize>(
            #vapp: [#core::driver::settings::Routes; N],
        ) -> #core::driver::settings::Routes {
            let mut #vapp = (#vapp, #core::driver::settings::Routes::EMPTY, 0);
            while #vapp.2 < N {
                #vapp.1 = #vapp.1.join(#vapp.0[#vapp.2]);
                #vapp.2 += 1;
            }
            #vapp.1
        }

        impl #layout_head #exec::executor::raw::Layout<#life>
            for #name #args #layout_clause
        {
            type ParkPlan = #plan;
        }

        /// SAFETY: each lifecycle operation visits all declared owners once,
        /// in declaration order, through the same pinned projections and presence.
        /// The root stays pinned through finish and retains each operation's authority.
        unsafe impl #head #exec::executor::raw::App<#life>
            for #name #args #clause
        {
            const ROUTES: #core::driver::settings::Routes = #vapp(#set);
            const WAKE_ROUTES: #core::driver::settings::Routes = #wake_set;

            #admit

            fn released<#app_life>(
                mut #vcall: #exec::executor::raw::Released<#app_life, Self>,
            ) -> #exec::executor::raw::Released<#app_life, Self> {
                #released
                #vcall
            }

            fn install<#app_life, #owned>(
                #vinstall: #exec::executor::raw::Install<#app_life, #owned, #life, Self>,
            ) -> #exec::executor::raw::Install<#app_life, #owned, #life, Self> {
                let (mut #vapp, mut #vinstall) = #vinstall.into_parts();
                #setup
                #exec::executor::raw::Install::new(#vapp, #vinstall)
            }

            fn dispatch<#app_life, #driver, #turn, #root>(
                #vdisp: #exec::executor::raw::Dispatch<
                    #app_life, #driver, #turn, #root, #life, Self,
                >,
            ) -> ::core::ops::ControlFlow<#core::io::Event<#life>>
            where
                #life: #root,
                Self: #root,
            {
                #routes
            }

            fn progress(
                #vapp: ::core::pin::Pin<&Self>,
                #vprog: #exec::executor::raw::Progress<'_, '_, #life>,
            ) -> #core::driver::schedule::Progress<#life> {
                #active
            }

            fn stopping(
                #vapp: ::core::pin::Pin<&Self>,
                #vprog: #exec::executor::raw::Progress<'_, '_, #life>,
            ) -> #core::driver::schedule::Progress<#life> {
                #stop
            }

            const COORDS: ::core::primitive::usize = #coords;

            fn select_park() -> ::core::option::Option<Self::ParkPlan> {
                ::core::option::Option::Some(#select)
            }

            fn prepare_park(
                #vapp: ::core::pin::Pin<&Self>,
                #vplan: &mut Self::ParkPlan,
            ) -> ::core::option::Option<#exec::executor::raw::Count> {
                #measure
            }

            /// # Safety
            /// The authority and prepared plan belong to this exact installed pinned root.
            unsafe fn park<#app_life, #driver, #turn, #root>(
                mut #vapp: ::core::pin::Pin<&#app_life mut Self>,
                #vplan: &mut Self::ParkPlan,
                mut #vpark: #exec::executor::raw::Park<
                    #driver, #turn, #root, #life,
                >,
            )
            where
                #life: #driver + #turn + #root,
            {
                #cursors
                #shares
                #park
                #(#ticks)*
            }

            fn shutdown<#app_life, #turn, #driver, #owned>(
                #vstop: #exec::executor::raw::Shutdown<
                    #app_life, #turn, #driver, #owned, #life, Self,
                >,
            ) -> #exec::executor::raw::Shutdown<
                #app_life, #turn, #driver, #owned, #life, Self,
            >
            where
                #life: #owned,
            {
                let (mut #vapp, mut #vturn, mut #vdriver) = #vstop.into_parts();
                #shutdown
                #exec::executor::raw::Shutdown::new(#vapp, #vturn, #vdriver)
            }

            fn finish<#app_life, #end, #owned, #epoch>(
                #vfinish: #exec::executor::raw::Finish<
                    #app_life, #end, #owned, #epoch, #life, Self,
                >,
            ) -> #exec::executor::raw::Finish<
                #app_life, #end, #owned, #epoch, #life, Self,
            > {
                let (mut #vapp, mut #vinstall, mut #vfinish) = #vfinish.into_parts();
                #finish
                #exec::executor::raw::Finish::new(#vapp, #vinstall, #vfinish)
            }

            /// # Safety
            /// The operation and selected route belong to this exact installed pinned root.
            unsafe fn activate<#app_life, #driver, #turn, #root>(
                mut #vapp: ::core::pin::Pin<&#app_life mut Self>,
                mut #vact: #exec::executor::raw::Activate<
                    #driver, #turn, #root, #life,
                >,
            ) -> bool
            where
                #life: #driver + #turn + #root,
            {
                let #vroute = #vact.target().route();
                #wcall
                false
            }
        }
    }
}

fn lifecycle(app: &Spec, owner: &Owner) -> [proc_macro2::TokenStream; 3] {
    let [vchild, vctx, vdriver, vfinish, vinstall, vroot, vturn] = [
        "__child",
        "__context",
        "__driver",
        "__finish",
        "__install",
        "__root",
        "__turn",
    ]
    .map(|name| app.brand.ident(name));
    let mf = app.paths.manifold();
    let exec = app.paths.runtime();
    let life = app.brand.lifetime();
    let inner = owner.inner();
    let view = Project::new(owner, app);
    let setup = view.map(quote::quote! { #vinstall }, |recv| {
        let install = if owner.is_app() {
            quote::quote! {
                <#inner as #exec::executor::raw::App<#life>>::install(
                    #vchild,
                ).into_install()
            }
        } else {
            quote::quote! {
                <<#inner as #mf::app::dispatch::raw::Manifold<#life>>::Install as
                    #mf::app::dispatch::raw::Installs<#life, #inner>>::call(
                        #vchild,
                ).into_install()
            }
        };
        quote::quote! {
            let #vchild = #exec::executor::raw::Install::new(#recv, #vinstall);
            #install
        }
    });
    let stop = view.map(quote::quote! { (#vturn, #vdriver) }, |recv| {
        if owner.is_app() {
            quote::quote! {
                let #vchild = #exec::executor::raw::Shutdown::new(
                    #recv,
                    #vturn,
                    #vdriver,
                );
                <#inner as #exec::executor::raw::App<#life>>::shutdown(#vchild).into_drive()
            }
        } else {
            quote::quote! {
                let mut #vroot =
                    #mf::app::dispatch::raw::Context::<
                        #mf::app::dispatch::raw::Retained,
                    >::new(#vdriver.reborrow());
                let mut #vctx = #vroot.narrow::<<#inner as
                    #mf::app::dispatch::raw::Manifold<#life>>::Shutdown>();
                #mf::app::dispatch::raw::Manifold::shutdown(
                    #recv,
                    #vturn.reborrow(),
                    &mut #vctx,
                );
                (#vturn, #vdriver)
            }
        }
    });
    let finish = view.map(quote::quote! { (#vinstall, #vfinish) }, |recv| {
        let finish = if owner.is_app() {
            quote::quote! {
                <#inner as #exec::executor::raw::App<#life>>::finish(#vchild).into_finish()
            }
        } else {
            quote::quote! {
                <<#inner as #mf::app::dispatch::raw::Manifold<#life>>::Finish as
                    #exec::executor::raw::Finishes<#life, #inner>>::call(
                        #vchild,
                ).into_finish()
            }
        };
        quote::quote! {
            let #vchild = #exec::executor::raw::Finish::new(
                #recv,
                #vinstall,
                #vfinish,
            );
            #finish
        }
    });

    [setup, stop, finish]
}
