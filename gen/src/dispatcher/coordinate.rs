use crate::dispatcher;

pub(super) struct Coordinate<'a> {
    application: &'a dispatcher::Application,
}

impl<'a> Coordinate<'a> {
    pub(super) fn new(application: &'a dispatcher::Application) -> Self {
        Self { application }
    }

    pub(super) fn projection(&self) -> proc_macro2::TokenStream {
        let application = self.application;
        if !application.coordinate {
            return quote::quote! {};
        }

        let name = &application.name;
        let coordinate = quote::format_ident!("{}Coordinate", name);
        let manifold = &application.paths.manifold;
        let runtime = &application.paths.runtime;
        let lifetime = &application.brand.lifetime;
        let (_, application_ty, _) = application.generics.split_for_impl();

        let mut generics = application.generics.clone();
        generics
            .params
            .insert(0, syn::parse_quote!('__coordinate_turn));
        generics
            .params
            .insert(0, syn::parse_quote!('__coordinate_step));
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote! { #lifetime: '__coordinate_step });
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote! { '__coordinate_turn: '__coordinate_step });
        for field in application.fields.iter().filter(|field| field.control) {
            let inner = field.manifold_ty();
            generics
                .make_where_clause()
                .predicates
                .push(syn::parse_quote! { #inner: #manifold::dispatch::raw::Controlled<#lifetime> + '__coordinate_step });
        }
        for state in &application.states {
            let ty = &state.ty;
            generics
                .make_where_clause()
                .predicates
                .push(syn::parse_quote! { #ty: '__coordinate_step });
        }

        let params = &generics.params;
        let where_clause = &generics.where_clause;
        let controls = application
            .fields
            .iter()
            .filter(|field| field.control)
            .map(|field| {
                let field_name = &field.name;
                let inner = field.manifold_ty();
                let control = quote::quote! {
                    <#inner as #manifold::dispatch::raw::Controlled<#lifetime>>::Control<'__coordinate_step>
                };
                let ty = if field.optional {
                    quote::quote! { ::core::option::Option<#control> }
                } else {
                    control
                };
                quote::quote! { #field_name: #ty }
            });
        let states = application.states.iter().map(|state| {
            let state_name = &state.name;
            let ty = &state.ty;
            quote::quote! { #state_name: &'__coordinate_step mut #ty }
        });

        quote::quote! {
            #[doc(hidden)]
            struct #coordinate <#params> #where_clause {
                #(#controls,)*
                #(#states,)*
                step: #runtime::coordinate::Step<
                    '__coordinate_step,
                    '__coordinate_turn,
                    #lifetime,
                >,
                _application: ::core::marker::PhantomData<
                    fn(#name #application_ty) -> #name #application_ty,
                >,
            }
        }
    }

    pub(super) fn call(&self) -> proc_macro2::TokenStream {
        let application = self.application;
        let coordinate = quote::format_ident!("{}Coordinate", application.name);
        let manifold = &application.paths.manifold;
        let runtime = &application.paths.runtime;
        let lifetime = &application.brand.lifetime;
        let controls = application
            .fields
            .iter()
            .filter(|field| field.control)
            .map(|field| {
                let field_name = &field.name;
                let inner = field.manifold_ty();
                let control = quote::quote! {
                    unsafe { <#inner as #manifold::dispatch::raw::Controlled<#lifetime>>::control(__owner) }
                };
                if field.optional {
                    quote::quote! {
                        #field_name: {
                            let __field = __projection.#field_name;
                            __field.as_pin_mut().map(|__owner| #control)
                        }
                    }
                } else {
                    quote::quote! {
                        #field_name: {
                            let __owner = __projection.#field_name;
                            #control
                        }
                    }
                }
            });
        let states = application.states.iter().map(|state| {
            let state_name = &state.name;
            quote::quote! { #state_name: __projection.#state_name }
        });
        quote::quote! {
            let mut __coordinate_budget = __turn.reborrow().coordination();
            loop {
                let ::core::option::Option::Some(__step) =
                    #runtime::coordinate::Step::try_new(
                        &mut __coordinate_budget,
                        &mut **__driver,
                    )
                else {
                    break;
                };
                let __projection = __app.as_mut().project();
                let __flow = Self::coordinate(#coordinate {
                    #(#controls,)*
                    #(#states,)*
                    step: __step,
                    _application: ::core::marker::PhantomData,
                });
                if ::core::matches!(__flow, #runtime::coordinate::Flow::Idle) {
                    break;
                }
            }
        }
    }
}
