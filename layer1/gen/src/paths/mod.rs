pub(crate) mod app;
mod package;

pub(crate) fn forward(
    facade: Option<proc_macro2::TokenStream>,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let root = package::Package::Dope.root(facade);
    (root.core(), root.manifold())
}

pub(crate) fn fiber(facade: Option<proc_macro2::TokenStream>) -> proc_macro2::TokenStream {
    package::Package::Fiber.root(facade).abi()
}
