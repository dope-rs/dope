use crate::app::{Spec, output::project::Project};

pub(crate) fn generate(app: &Spec, stop: bool) -> proc_macro2::TokenStream {
    let [vacc, vapp, vclock, vtime, vprog] =
        ["__acc", "__app", "__clock", "__deadline", "__progress"].map(|name| app.brand.ident(name));
    let core = app.paths.core();
    let mf = app.paths.manifold();
    let exec = app.paths.runtime();
    let life = app.brand.lifetime();
    let method = if stop {
        quote::format_ident!("stopping")
    } else {
        quote::format_ident!("progress")
    };
    let sched = if stop {
        Vec::new()
    } else {
        app.states
            .iter()
            .filter(|state| state.is_scheduled())
            .map(|state| {
                let name = state.name();
                let ty = state.ty();
                quote::quote! {
                    if let ::core::option::Option::Some(#vtime) =
                        <#ty as #mf::timing::Schedule<#life>>::deadline(
                            &#vapp.as_ref().get_ref().#name,
                            #vclock,
                        )
                    {
                        #vacc = #vacc.reduce(
                            #core::driver::schedule::Progress::until(#vtime),
                        );
                    }
                }
            })
            .collect::<Vec<_>>()
    };
    if app.owners.is_empty() && sched.is_empty() {
        return quote::quote! {
            #core::driver::schedule::Progress::Quiescent
        };
    }
    let inputs = if sched.is_empty() {
        quote::quote! {}
    } else {
        quote::quote! { let #vclock = #vprog.clock(); }
    };
    let owners = app.owners.iter().map(|owner| {
        let inner = owner.inner();
        Project::new(owner, app).shared(|recv| {
            let call = if owner.is_app() {
                quote::quote! {
                    <#inner as #exec::executor::raw::App<#life>>::#method(
                        #recv,
                        #vprog,
                    )
                }
            } else {
                quote::quote! {
                    <<#inner as #mf::app::dispatch::raw::Manifold<#life>>::Probe as
                        #mf::app::dispatch::raw::Probe<#life, #inner>>::#method(
                        #recv,
                        #vprog.regions(),
                        #vprog.clock(),
                    )
                }
            };
            quote::quote! {
                match #call {
                    #core::driver::schedule::Progress::Runnable => {
                        return #core::driver::schedule::Progress::Runnable;
                    }
                    #vprog => #vacc = #vacc.reduce(#vprog),
                }
            }
        })
    });
    quote::quote! {
        {
            #inputs
            let mut #vacc = #core::driver::schedule::Progress::Quiescent;
            #(#owners)*
            #(#sched)*
            #vacc
        }
    }
}
