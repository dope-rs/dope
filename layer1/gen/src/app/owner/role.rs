pub(super) enum Role {
    App,
    Manifold { controlled: bool, route: syn::Ident },
}
