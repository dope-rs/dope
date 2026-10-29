use proc_macro2::TokenStream;

use crate::{Failure, failure::Work, paths::app::App, tokens};

fn paths(input: &syn::DeriveInput) -> Result<(TokenStream, TokenStream), Failure> {
    let mut core = None;
    let mut runtime = None;
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("work"))
    {
        let syn::Meta::List(list) = &attr.meta else {
            return Err(Failure::new(attr, Work::Setting));
        };
        for value in tokens::values(list, |span| Failure::at(span, Work::Setting))? {
            if value.name == "core" {
                if core.replace(value.value.tokens).is_some() {
                    return Err(Failure::new(&value.name, Work::DuplicateCore));
                }
            } else if value.name == "runtime" {
                if runtime.replace(value.value.tokens).is_some() {
                    return Err(Failure::new(&value.name, Work::DuplicateRuntime));
                }
            } else {
                return Err(Failure::new(&value.name, Work::Setting));
            }
        }
    }
    let default = App::discover();
    Ok((
        core.unwrap_or_else(|| default.core().clone()),
        runtime.unwrap_or_else(|| default.runtime().clone()),
    ))
}

pub(crate) fn expand(input: syn::DeriveInput) -> proc_macro::TokenStream {
    match generate(input) {
        Ok(tokens) => tokens.into(),
        Err(failure) => failure.emit(),
    }
}

fn generate(input: syn::DeriveInput) -> Result<proc_macro2::TokenStream, Failure> {
    let (core, runtime) = paths(&input)?;
    if !input.generics.params.is_empty() {
        return Err(Failure::new(&input.generics, Work::Generics));
    }
    let source = quote::quote!(#input);
    let value = tokens::ident(&source, "T");
    let [done, at, ready] = ["done", "slot", "ready"].map(|name| tokens::ident(&source, name));
    let vars = match input.data {
        syn::Data::Enum(data) => data.variants,
        syn::Data::Struct(data) => {
            return Err(Failure::new(&data.struct_token, Work::Enum));
        }
        syn::Data::Union(data) => {
            return Err(Failure::new(&data.union_token, Work::Enum));
        }
    };
    for var in &vars {
        if !matches!(var.fields, syn::Fields::Unit) {
            return Err(Failure::new(var, Work::Field));
        }
    }
    if !(2..=256).contains(&vars.len()) {
        return Err(Failure::new(&vars, Work::Count));
    }
    let Some(first) = vars.first() else {
        return Err(Failure::new(&vars, Work::Count));
    };
    let first = &first.ident;

    let name = input.ident;
    let graph = quote::format_ident!("{}Graph", name);
    let cycle = quote::format_ident!("__{}Cycle", name);
    let cycle = tokens::ident(&source, &cycle.to_string());
    let vis = input.vis;
    let names: Vec<_> = vars.iter().map(|var| &var.ident).collect();
    let slots = names.len();
    let next = names
        .iter()
        .zip(names.iter().cycle().skip(1))
        .map(|(at, next)| quote::quote!(#cycle::#at => #cycle::#next,));
    let slot = names
        .iter()
        .map(|var| quote::quote!(#name::#var => #cycle::#var,));
    let take = names
        .iter()
        .map(|var| quote::quote!(#cycle::#var => #name::#var,));
    // The private scope isolates items; its live functions keep caller constants
    // from capturing generated parameter and closure bindings.
    let out = quote::quote! {
        #vis type #graph = <#name as #runtime::executor::work::Work>::Graph;

        const _: () = {
            #[derive(
                ::core::clone::Clone,
                ::core::marker::Copy,
                ::core::cmp::Eq,
                ::core::cmp::PartialEq,
            )]
            #[repr(u8)]
            enum #cycle {
                #(#names,)*
            }

            fn #done(#at: #name) -> #cycle {
                match #at {
                    #(#slot)*
                }
            }

            fn #at(#done: #cycle) -> #name {
                match #done {
                    #(#take)*
                }
            }

            fn #ready(#done: #cycle) -> #cycle {
                match #done {
                    #(#next)*
                }
            }

            impl #runtime::executor::work::Cycle for #cycle {
                const FIRST: Self = Self::#first;

                fn next(self) -> Self {
                    #ready(self)
                }
            }

            #[repr(transparent)]
            pub struct #graph {
                arbiter: #runtime::executor::work::Arbiter<#cycle>,
            }

            impl #runtime::executor::work::Work for #name {
                type Graph = #graph;
            }

            impl #graph {
                #vis const SLOTS: ::core::primitive::usize = #slots;

                #vis fn new() -> Self {
                    Self {
                        arbiter: #runtime::executor::work::Arbiter::new(),
                    }
                }

                /// Takes the current slot and advances once.
                #vis fn take(&mut self) -> #name {
                    #at(self.arbiter.take())
                }

                #vis fn account(&mut self, #at: #name) {
                    self.arbiter.account(#done(#at));
                }

                #vis fn find(
                    &self,
                    mut #ready: impl ::core::ops::FnMut(#name) -> ::core::primitive::bool,
                ) -> ::core::option::Option<#name> {
                    self.arbiter
                        .find(|#done| #ready(#at(#done)))
                        .map(#at)
                }

                #vis fn select(
                    &mut self,
                    mut #ready: impl ::core::ops::FnMut(#name) -> ::core::primitive::bool,
                ) -> ::core::option::Option<#name> {
                    self.arbiter
                        .select(|#done| #ready(#at(#done)))
                        .map(#at)
                }

                #vis fn admit<#value>(
                    &mut self,
                    mut #ready: impl ::core::ops::FnMut(#name) -> #core::o3::mem::quota::Admission<#value>,
                ) -> #core::o3::mem::quota::Admission<#value> {
                    self.arbiter.admit(|#done| #ready(#at(#done)))
                }
            }

            impl ::core::default::Default for #graph {
                fn default() -> Self {
                    Self::new()
                }
            }
        };

        const _: () = ::core::assert!(
            ::core::mem::size_of::<#graph>() == 1
        );
    };
    Ok(out)
}
