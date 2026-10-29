use core::convert::Infallible;
use std::cell;

use bindings::endpoint;
use dope::{
    core::{
        Errno,
        driver::{self, route::Identity, schedule::app::Work},
    },
    manifold::{
        client::{
            app::{self, connect::Peer, failure, request},
            attempt, close, conn,
            session::{app::drain::Ready, codec::mode, policy, protocol, response, turns},
        },
        service::bindings,
    },
    net::link::{egress, pool::change::opening},
};
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer,
    buffer::hold::scope::Deferred,
};
use policy::output;
use stream::cursor::Cursor;

use crate::{
    lookup::error,
    net::{
        entropy, stream,
        stream::{
            codec,
            link::{Bind, Link, Retiring},
        },
    },
    storage::{
        self,
        queries::{
            self, index,
            stream::{Admission, Frame, Submission},
        },
    },
};

type Owner<'life, 's, 'd, L, ID, const LANES: usize, const N: usize> =
    endpoint::State<'life, 'd, storage::State<'s, 'd, LANES, N>, L, ID, CONNECTIONS>;

type Context<'a, 't, 'd, ID> = turns::Context<'a, 't, 'd, (), Frame, ID>;

const CONNECTIONS: usize = stream::CONNECTIONS;

pub(crate) struct Logic<
    'life,
    's,
    'd: 'life,
    ID: Identity,
    const IDS: u32,
    const LANES: usize,
    const N: usize,
> {
    codec: codec::Prefix,
    random: entropy::Stream<'d>,
    link: cell::Cell<Option<Link<'life, 's, 'd, ID, IDS, LANES, N>>>,
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    pub(crate) const fn new(random: entropy::Stream<'d>) -> Self {
        Self {
            codec: codec::Prefix,
            random,
            link: cell::Cell::new(None),
        }
    }

    fn bind(&self, token: conn::Id<'d, ID>) -> Option<Bind<'life, 's, 'd, ID, IDS, LANES, N>> {
        let Some(Link::Open(bind)) = self.link.get() else {
            return None;
        };
        (bind.token == token).then_some(bind)
    }

    fn retire(&self, bind: Bind<'life, 's, 'd, ID, IDS, LANES, N>, error: Option<error::Kind>) {
        self.link.set(Some(Link::Retiring(Retiring {
            bind,
            branches: index::Retirement::new(),
            error,
        })));
    }

    fn reject(ctx: &mut Context<'_, '_, 'd, ID>) {
        ctx.request(close::Directive::reconnect(close::Reason::Protocol));
    }

    fn close(
        bind: Bind<'life, 's, 'd, ID, IDS, LANES, N>,
        status: queries::StreamStatus,
    ) -> Option<close::Reason> {
        if status.live && !status.live_for_server {
            return Some(close::Reason::EndpointRetired);
        }
        if bind.ids.next().is_none() && status.pending_for_server && !status.transactions {
            return Some(close::Reason::Local(close::LocalOrigin::ScheduledRotation));
        }
        None
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    protocol::Spec<'life, 'd, storage::State<'s, 'd, LANES, N>, ID, CONNECTIONS>
    for Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    type Codec = codec::Prefix;
    type Mode = mode::Plain;
    type Requests = request::Enabled;
    type Response = response::Immediate;
    type Connect = Peer;
    type ConnState = ();
    type Failure = failure::Controlled;
    type Send = Frame;

    fn codec<'a>(owner: &'a Owner<'life, 's, 'd, Self, ID, LANES, N>) -> &'a Self::Codec {
        &owner.logic().codec
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    policy::Exhausted<'life, 'd, storage::State<'s, 'd, LANES, N>, ID, CONNECTIONS>
    for Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    fn exhaust(
        owner: &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        owner
            .root()
            .queries
            .exhaust_tcp(driver.state_token(), error::Kind::Opening(cause));
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    protocol::Plain<'life, 'd, storage::State<'s, 'd, LANES, N>, ID, CONNECTIONS>
    for Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    fn reject(
        owner: &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        cause: codec::Error,
        ctx: &mut Context<'_, '_, 'd, ID>,
    ) -> response::Direct {
        let logic = owner.logic();
        if let Some(bind) = logic.bind(ctx.conn_id) {
            logic.retire(bind, Some(error::Kind::Stream(cause)));
        }
        Self::reject(ctx);
        response::Direct::Committed
    }

    fn deliver<'input, R: buffer::Cursor>(
        owner: &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        frame: Deferred<'input, 'd, R>,
        ctx: &mut Context<'_, '_, 'd, ID>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        let logic = owner.logic();
        let Some(bind) = logic.bind(ctx.conn_id) else {
            Self::reject(ctx);
            return response::Direct::Committed;
        };
        let now = ctx.now();
        let status =
            match bind
                .peer
                .try_recv(ctx.parts().driver.state_token(), frame.as_slice(), now)
            {
                Ok(status) => status,
                Err(cause) => {
                    logic.retire(
                        bind,
                        Some(error::Kind::Malformed(error::Malformed::Packet(cause))),
                    );
                    Self::reject(ctx);
                    return response::Direct::Committed;
                }
            };
        if let Some(reason) = Self::close(bind, status) {
            ctx.request(close::Directive::reconnect(reason));
        }
        response::Direct::Committed
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    policy::Output<'life, 'd, storage::State<'s, 'd, LANES, N>, ID, CONNECTIONS>
    for Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    fn sent() -> impl for<'a, 'x> Call<(
        &'a mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        conn::Id<'d, ID>,
        egress::Pending,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<Infallible, Infallible, _>::Pending(
            |(owner, conn, pending, token): (
                &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
                conn::Id<'d, ID>,
                egress::Pending,
                &mut region::Token<region::Identity<'d, 1>>,
            )| {
                let logic = owner.logic();
                if pending == egress::Pending::Absent
                    && let Some(bind) = logic.bind(conn)
                    && bind.peer.status(token).pending_for_server
                {
                    bind.wake.wake();
                }
            },
        )
    }

    fn flush() -> impl for<'a, 'x, 'y, 'z> Call<(
        &'a mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        &'x mut Context<'y, 'z, 'd, ID>,
    )> {
        output::Flush::<Infallible, _>::Context(
            |(owner, ctx): (
                &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
                &mut Context<'_, '_, 'd, ID>,
            )| {
                let logic = owner.logic();
                if let Some(bind) = logic.bind(ctx.conn_id)
                    && let Some(reason) =
                        Self::close(bind, bind.peer.status(ctx.parts().driver.state_token()))
                {
                    ctx.request(close::Directive::reconnect(reason));
                }
            },
        )
    }

    fn cancel() -> impl for<'a, 'x> Call<(
        &'a Owner<'life, 's, 'd, Self, ID, LANES, N>,
        Self::Send,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Cancel::<Infallible, Infallible, _>::Context(
            |(owner, payload, token): (
                &Owner<'life, 's, 'd, Self, ID, LANES, N>,
                Self::Send,
                &mut region::Token<region::Identity<'d, 1>>,
            )| {
                let id = payload.cancel();
                owner.root().queries.undo_stream(token, id);
            },
        )
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    policy::Requests<'life, 'd, storage::State<'s, 'd, LANES, N>, ID, CONNECTIONS>
    for Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    fn drain(
        owner: &Owner<'life, 's, 'd, Self, ID, LANES, N>,
        reqs: &mut request::Requests<'_, '_, '_, 'd, Self::Send, ID>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let logic = owner.logic();
        let Some(mut bind) = logic.bind(reqs.connection()) else {
            reqs.close(close::Directive::reconnect(close::Reason::EndpointRetired));
            return;
        };
        let now = reqs.now();
        bind.blocked = false;
        while let Some((id, next)) = bind.ids.next() {
            let sent = bind.peer.submit(token, now, id, |frame| {
                let (frame, commit) = frame.prepare();
                let frame = match egress::Load::new(frame).try_nonempty() {
                    Ok(frame) => frame,
                    Err(rejected) => return Admission::Rejected(rejected),
                };
                match reqs.admit(frame) {
                    request::Admission::Ready(ready) => {
                        ready.push();
                        Admission::Committed(commit)
                    }
                    request::Admission::Exhausted(frame) => Admission::Exhausted(frame),
                    request::Admission::Saturated(load) => {
                        bind.blocked = true;
                        Admission::Saturated(load)
                    }
                    request::Admission::Rejected(rejected) => Admission::Unavailable(rejected),
                }
            });
            match sent {
                Submission::Committed | Submission::Rejected => {
                    bind.ids = next;
                }
                Submission::Deferred => break,
            }
        }
        logic.link.set(Some(Link::Open(bind)));
        if let Some(reason) = Self::close(bind, bind.peer.status(token)) {
            reqs.close(close::Directive::reconnect(reason));
        }
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    policy::Peer<'life, 'd, storage::State<'s, 'd, LANES, N>, ID, CONNECTIONS>
    for Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    type Addr = index::Server;

    fn connect(
        owner: &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        server: Self::Addr,
        ctx: &mut Context<'_, '_, 'd, ID>,
    ) {
        let store = owner.root();
        let logic = owner.logic_mut();
        let peer = store.peer(server);
        let start = logic.random.start(&ctx.conn_id);
        logic.link.set(Some(Link::Open(Bind {
            token: ctx.conn_id,
            peer,
            wake: ctx.wake_target(),
            ids: Cursor::new(start),
            blocked: false,
        })));
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    policy::Lifecycle<'life, 'd, storage::State<'s, 'd, LANES, N>, ID, CONNECTIONS>
    for Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    type Drain = Ready;

    fn quarantine(
        owner: &Owner<'life, 's, 'd, Self, ID, LANES, N>,
        conn: conn::Id<'d, ID>,
        _: &mut Self::ConnState,
        reason: close::Reason,
        token: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let logic = owner.logic();
        let Some(link) = logic.link.get().filter(|link| link.token() == conn) else {
            return;
        };
        let cause = error::Kind::Close { reason };
        if cause.is_terminal() {
            owner.root().queries.exhaust_tcp(&mut token.1, cause);
        }
        if let Link::Open(bind) = link {
            logic.retire(bind, None);
        }
    }

    fn io_fail(
        owner: &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        error: Errno,
        ctx: &mut Context<'_, '_, 'd, ID>,
    ) {
        let logic = owner.logic();
        if let Some(bind) = logic.bind(ctx.conn_id) {
            logic.retire(bind, Some(error::Kind::Transport { errno: error }));
        }
    }

    fn clear_recv<'turn>(
        owner: &Owner<'life, 's, 'd, Self, ID, LANES, N>,
        conn: conn::Id<'d, ID>,
        reason: close::Reason,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let logic = owner.logic();
        let Some(Link::Retiring(retiring)) = logic.link.get() else {
            return egress::Cleared::Done;
        };
        if retiring.bind.token != conn {
            return egress::Cleared::Done;
        }
        let (retiring, progress) = retiring.clear(reason, ctx);
        logic.link.set(retiring.map(Link::Retiring));
        progress
    }

    fn disconnect(
        owner: &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        ctx: turns::Retired<'_, 'd, Self::ConnState, ID, Owner<'life, 's, 'd, Self, ID, LANES, N>>,
    ) {
        let logic = owner.logic_mut();
        let conn = ctx.connection();
        if logic.link.get().is_some_and(|link| link.token() == conn) {
            logic.link.set(None);
        }
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const IDS: u32, const LANES: usize, const N: usize>
    policy::Scheduling<'life, 'd, storage::State<'s, 'd, LANES, N>, ID, CONNECTIONS>
    for Logic<'life, 's, 'd, ID, IDS, LANES, N>
{
    fn park<'turn>(
        owner: &mut Owner<'life, 's, 'd, Self, ID, LANES, N>,
        work: Work<'turn, 'd>,
        token: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let logic = owner.logic_mut();
        let Some(Link::Open(bind)) = logic.link.get() else {
            return;
        };
        let status = bind.peer.status(&mut token.1);
        let pending = status.pending_for_server && !bind.blocked && bind.ids.next().is_some();
        if (pending || Self::close(bind, status).is_some()) && work.permit().is_some() {
            bind.wake.wake();
        }
    }

    fn shutdown(owner: &mut Owner<'life, 's, 'd, Self, ID, LANES, N>) {
        if let Some(Link::Open(bind)) = owner.logic().link.get() {
            bind.wake.wake();
        }
    }

    fn inbound(
        owner: &Owner<'life, 's, 'd, Self, ID, LANES, N>,
        _: conn::Id<'d, ID>,
        _: &Self::ConnState,
        _: Positive,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        app::Inbound::Awaiting(owner.root().queries.config.query.attempt_timeout)
    }
}
