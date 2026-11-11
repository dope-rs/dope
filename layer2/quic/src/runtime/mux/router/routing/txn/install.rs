use crate::runtime::{brand, routing::registry};
use std::pin;

use crate::runtime::mux::handler::hook::life::create;
use crate::{conn, runtime::mux, stream};
use conn::{id, scope};
use create::Mode as _;
use mux::{auth, handler, router};
use router::routing;
use routing::txn;
use txn::plan;

pub(in crate::runtime::mux) enum Outcome<I> {
    Installed(I),
    Rejected,
    Unavailable,
}

pub(in crate::runtime::mux) fn try_attach<
    'storage,
    'id,
    'scope,
    A: auth::Mode<DOMAIN>,
    H,
    const DOMAIN: u8,
    B,
    R,
>(
    mut handler: pin::Pin<&mut H>,
    binding: &mut router::Binding<A, DOMAIN>,
    scope: brand::Token<'scope>,
    plan: plan::Plan<'storage, 'id, H::Connection, A::Session, DOMAIN, B, A::Send>,
    apply: impl FnOnce(
        pin::Pin<&mut H>,
        &mut router::Binding<A, DOMAIN>,
        &mut registry::Scope<'storage, 'id, H::Connection, DOMAIN, B, A::Send, A::Session>,
        registry::Key<'id>,
        id::Handle<A::Identity, DOMAIN>,
    ) -> R,
) -> Result<Outcome<R>, conn::Timing>
where
    B: stream::Recv,
    H: handler::Handler<DOMAIN, B, A::Send, A::Identity>,
{
    let plan::Plan {
        connection: mut conn,
        tls,
        peer_addr: peer,
        max_packet_bytes: limit,
        now,
        routes,
    } = plan;
    conn.machine_mut().try_open(now, binding.opening_lifetime)?;
    conn.scope_in(scope.reborrow(), |mut conn| {
        conn.routing().bind_peer(peer);
    });
    Ok(routes
        .claim(binding.domain)
        .map(|entry| {
            let handle = entry.handle();
            conn.mark(conn::Effects::ROUTING);
            let app = match H::Create::create(create::Event::new(
                handler.as_mut(),
                scope::Access::new(conn.machine_mut(), scope.reborrow()),
                handle,
            )) {
                create::Decision::Install(app) => app,
                create::Decision::Reject => {
                    entry.reject();
                    return Outcome::Rejected;
                }
            };
            let slot = registry::Slot::new(conn, tls, app, peer, limit);
            entry.install(slot, |slots, exact| {
                router::State::<A, H::Connection, DOMAIN, B>::reconcile(
                    scope, slots, exact, binding, now, None,
                );
                Outcome::Installed(apply(handler, binding, slots, exact, handle))
            })
        })
        .unwrap_or(Outcome::Unavailable))
}
