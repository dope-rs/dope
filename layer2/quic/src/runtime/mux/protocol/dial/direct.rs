use crate::runtime::brand;

use std::{net, time};

use crate::{
    conn, errors,
    runtime::{mux, routing},
    stream,
    wire::packet::cid,
};
use conn::{id, life};
use life::{options, tls::clients};
use mux::{auth, handler, protocol};
use protocol::dial::{self, setup};

impl<'a, 'tls, H, const DOMAIN: u8, B, A> protocol::Io<'a, 'tls, H, DOMAIN, B, A>
where
    A: auth::Mode<DOMAIN>,
    H: handler::Handler<DOMAIN, B, A::Send, A::Identity>,
    B: stream::Recv,
{
    pub(crate) fn connect(
        &mut self,
        peer: net::SocketAddr,
        auth: &clients::Authority,
        options: options::Options,
        dcid: Vec<u8>,
        now: time::Instant,
    ) -> Result<id::Handle<A::Identity, DOMAIN>, errors::Connect> {
        let dcid = cid::Id::try_from(dcid)?;
        brand::Token::with(|scope| self.connect_in(scope, peer, auth, options, dcid, now))
    }

    pub(crate) fn connect_in(
        &mut self,
        scope: brand::Token<'_>,
        peer: net::SocketAddr,
        auth: &clients::Authority,
        options: options::Options,
        dcid: cid::Id,
        now: time::Instant,
    ) -> Result<id::Handle<A::Identity, DOMAIN>, errors::Connect> {
        setup::Setup::with(self, |setup| {
            if !routing::PeerRoute::matches(routing::Plain, &dcid) {
                return Err(errors::Connect::InvalidConfig);
            }
            let ready = setup.reserve(&options)?;
            ready.finish(
                scope,
                auth.try_clone()?,
                options,
                dial::Prepared {
                    route: routing::Plain,
                    initial_dcid: dcid,
                    peer_addr: peer,
                    now,
                    token: None,
                },
                None,
            )
        })
    }

    pub(crate) fn connect_with<R: routing::PeerRoute>(
        &mut self,
        peer: net::SocketAddr,
        auth: &clients::Authority,
        options: options::Options,
        route: R,
        now: time::Instant,
    ) -> Result<id::Handle<A::Identity, DOMAIN>, errors::Connect> {
        brand::Token::with(|scope| self.route_in(scope, peer, auth, options, route, now))
    }

    pub(crate) fn route_in<R: routing::PeerRoute>(
        &mut self,
        scope: brand::Token<'_>,
        peer: net::SocketAddr,
        auth: &clients::Authority,
        options: options::Options,
        route: R,
        now: time::Instant,
    ) -> Result<id::Handle<A::Identity, DOMAIN>, errors::Connect> {
        setup::Setup::with(self, |setup| {
            let (dcid, ready) = setup.issue(&options, route)?;
            ready.finish(
                scope,
                auth.try_clone()?,
                options,
                dial::Prepared {
                    route,
                    initial_dcid: dcid,
                    peer_addr: peer,
                    now,
                    token: None,
                },
                None,
            )
        })
    }

    pub(crate) fn reconnect<R: routing::PeerRoute>(
        &mut self,
        options: options::Options,
        again: life::Reconnect,
        route: R,
        now: time::Instant,
    ) -> Result<id::Handle<A::Identity, DOMAIN>, errors::Reconnect> {
        brand::Token::with(|scope| self.reconnect_in(scope, options, again, route, now))
    }

    pub(crate) fn reconnect_in<R: routing::PeerRoute>(
        &mut self,
        scope: brand::Token<'_>,
        options: options::Options,
        again: life::Reconnect,
        route: R,
        now: time::Instant,
    ) -> Result<id::Handle<A::Identity, DOMAIN>, errors::Reconnect> {
        setup::Setup::with(self, |setup| {
            if !again.matches_path(route) {
                return Err(errors::Reconnect::retained(
                    errors::Preflight::InvalidConfig,
                    again,
                ));
            }
            let (dcid, ready) = match setup.issue(&options, route) {
                Ok(plan) => plan,
                Err(error) => return Err(errors::Reconnect::retained(error, again)),
            };
            let peer = again.peer();
            let (token, auth) = again.into_parts();
            ready
                .finish(
                    scope,
                    auth,
                    options,
                    dial::Prepared {
                        route,
                        initial_dcid: dcid,
                        peer_addr: peer,
                        now,
                        token: Some(token),
                    },
                    None,
                )
                .map_err(errors::Reconnect::consumed)
        })
    }
}
