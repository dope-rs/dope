use crate::client;

/// Produces a client from the exact selected provider.
/// The provider type carries its storage loans; each client requires those loans
/// and its driver domain to outlive the issued application scope.
pub trait Provider<'d> {
    type Client<'app>: 'app
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app;
}
