use crate::{manifold::app::dispatch::identity, net::port::entry};

pub(crate) struct Requests<'a, 'd> {
    pub(crate) send: Option<entry::Staged<'a, 'd>>,
    pub(crate) close: bool,
}

pub(in crate::net) struct Deferred<'a, 'd, I: identity::Identity> {
    pub(in crate::net) token: I,
    pub(in crate::net) requests: Requests<'a, 'd>,
}
