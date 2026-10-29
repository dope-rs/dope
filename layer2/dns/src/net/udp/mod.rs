use std::{net, pin};

use dope::{
    core::{
        driver::{
            self,
            route::Identity,
            schedule::{self, app, clock},
        },
        io::{
            datagram::{ReceiveError, source::Source},
            transfer::Length as _,
        },
    },
    manifold::{
        app::creation,
        network::datagram::{
            self,
            hook::{self, ingress},
            packet, plan, send,
        },
    },
};
use o3::{
    base::cell::region,
    buffer::pool::{self, owners, state},
    mem::quota,
};

use crate::{
    Report, config,
    lookup::error,
    net::entropy,
    storage::{
        self,
        queries::{self, receive},
    },
    wire::frames,
};

mod raw;

pub(crate) type Pool =
    pool::Pool<state::Uninit, owners::Plain<pool::Width<{ frames::MAX_QUERY_BYTES }>>>;

#[pin_project::pin_project]
#[derive(dope::Forward)]
pub(crate) struct Udp<
    'life,
    's,
    'd: 'life,
    ID: Identity,
    const LANES: usize,
    const ADDRESSES: usize,
    R: Report,
> {
    #[pin]
    #[forward('d)]
    inner: datagram::Endpoint<
        'd,
        ID,
        State<'life, 's, 'd, LANES, ADDRESSES, R>,
        frames::Datagram,
        send::Sourced,
    >,
}

impl<'life, 's, 'd: 'life, ID: Identity, const LANES: usize, const ADDRESSES: usize, R: Report>
    Udp<'life, 's, 'd, ID, LANES, ADDRESSES, R>
{
    pub(crate) fn bind(
        plan: plan::Plan<ID, send::Sourced>,
        pool: Pool,
        db: &'life storage::State<'s, 'd, LANES, ADDRESSES>,
        addr: net::SocketAddr,
        rng: entropy::Datagram<'d>,
        report: R,
        ctx: &mut driver::Context<'_, 'd>,
    ) -> Result<Self, creation::Error> {
        let servers = &db.queries.config.servers;
        let first = Source::try_new(addr, servers.first().address())
            .map_err(creation::Error::DatagramSource)?;
        let mut sources = [first; config::MAX_SERVERS];
        for (server, peer) in servers
            .map(|server, peer| (server, peer))
            .into_iter()
            .skip(1)
        {
            *server.get_mut(&mut sources) =
                Source::try_new(addr, peer).map_err(creation::Error::DatagramSource)?;
        }
        let state = State {
            db,
            pool,
            rng,
            report,
            sources,
        };
        let inner = plan.prepare(ctx)?.bind(addr, state, ctx)?;
        db.queries.bind_udp(ctx.state_token());
        Ok(Self { inner })
    }
}

#[pin_project::pin_project]
pub(crate) struct State<'life, 's, 'd: 'life, const LANES: usize, const ADDRESSES: usize, R: Report>
{
    db: &'life storage::State<'s, 'd, LANES, ADDRESSES>,
    pool: Pool,
    rng: entropy::Datagram<'d>,
    report: R,
    sources: [Source; config::MAX_SERVERS],
}

impl<'life, 's, 'd: 'life, const LANES: usize, const ADDRESSES: usize, R: Report>
    State<'life, 's, 'd, LANES, ADDRESSES, R>
{
    fn send<ID: Identity>(
        &mut self,
        mut sock: pin::Pin<&mut datagram::Socket<'d, ID, frames::Datagram, send::Sourced>>,
        now: clock::Moment<'d>,
        work: app::Work<'_, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let db = self.db;
        if db.queries.cleanup(token, work).is_break() {
            return;
        }
        let pool = &self.pool;
        let rng = &mut self.rng;
        let sources = &self.sources;
        loop {
            let Some(buf) = pool.cursor() else {
                return;
            };
            let next = db.apply(now, |set, raw| {
                set.admit_udp(token, raw, work, buf, rng, |buf, server| {
                    match sock.as_mut().queue(buf, *server.get(sources)) {
                        Ok(()) => queries::Submission::Committed,
                        Err(rejected) => {
                            let cause = rejected.cause();
                            match cause {
                                datagram::RejectCause::QueueFull
                                | datagram::RejectCause::Resident { .. } => {
                                    queries::Submission::Deferred
                                }
                                cause @ (datagram::RejectCause::Inactive
                                | datagram::RejectCause::IdentityExhausted
                                | datagram::RejectCause::Submit(_)
                                | datagram::RejectCause::Transfer(_)
                                | datagram::RejectCause::BudgetMissing { .. }
                                | datagram::RejectCause::Slot { .. }
                                | datagram::RejectCause::GsoUnavailable
                                | datagram::RejectCause::GsoShape(_)) => {
                                    queries::Submission::Failed(error::Kind::Datagram(
                                        error::Datagram::Rejected(cause),
                                    ))
                                }
                            }
                        }
                    }
                })
            });
            match next {
                quota::Admission::Item(queries::Submission::Committed) => {}
                quota::Admission::Item(queries::Submission::Deferred)
                | quota::Admission::Item(queries::Submission::Failed(_))
                | quota::Admission::Empty
                | quota::Admission::Exhausted => return,
            }
        }
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const LANES: usize, const ADDRESSES: usize, R: Report>
    datagram::Ingress<'d, ID> for State<'life, 's, 'd, LANES, ADDRESSES, R>
{
    type Hooks = hook::Set<
        hook::Disabled,
        hook::Disabled,
        hook::Disabled,
        hook::Transit,
        hook::Enabled,
        frames::Datagram,
        send::Sourced,
    >;
    type Mode = ingress::Time;
}

/// Receive failures carry no peer or transaction identity. Report the exact
/// transport cause while live requests retain their original attempts and deadlines.
impl<'life, 's, 'd: 'life, ID: Identity, const LANES: usize, const ADDRESSES: usize, R: Report>
    datagram::Time<'d, ID> for State<'life, 's, 'd, LANES, ADDRESSES, R>
{
    fn packet<'turn>(
        self: pin::Pin<&mut Self>,
        packet: Result<(net::SocketAddr, packet::Packet<'turn, 'd>), ReceiveError>,
        now: clock::Moment<'d>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let (addr, pkt) = match packet {
            Ok(packet) => packet,
            Err(cause) => {
                self.get_mut().report.receive(cause);
                return;
            }
        };
        let this = self.get_mut();
        this.db.apply(now, |set, raw| {
            receive::udp(set, token, addr, pkt.as_ref(), raw);
        });
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const LANES: usize, const ADDRESSES: usize, R: Report>
    datagram::Settlement<'d, ID> for State<'life, 's, 'd, LANES, ADDRESSES, R>
{
    fn settle<'turn>(
        self: pin::Pin<&mut Self>,
        done: send::Settlement<'turn, 'd, ID, frames::Datagram>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let this = self.get_mut();
        let (_, addr, state, data, at) = done.into_parts();
        let send::Payload::Buffer(datagram) = data else {
            return;
        };
        let expected = datagram.len();
        let ((id, epoch), cause) = {
            let datagram = datagram.into_inner();
            let cause = match state {
                send::Outcome::Sent(sent) if expected == sent => return,
                send::Outcome::Sent(sent) => {
                    error::Kind::Datagram(error::Datagram::Partial { sent, expected })
                }
                send::Outcome::Failed(errno) => error::Kind::Transport { errno },
                send::Outcome::Submit(error) => {
                    error::Kind::Datagram(error::Datagram::Submit(error))
                }
                send::Outcome::Shutdown => error::Kind::Datagram(error::Datagram::Shutdown),
            };
            (datagram.transaction(), cause)
        };
        this.db.apply(at, |set, raw| {
            set.fail_udp(token, id, epoch, addr, cause, raw);
        });
    }
}

impl<'life, 's, 'd: 'life, ID: Identity, const LANES: usize, const ADDRESSES: usize, R: Report>
    datagram::Transport<'d, ID> for State<'life, 's, 'd, LANES, ADDRESSES, R>
{
    fn pre_park(
        self: pin::Pin<&mut Self>,
        sock: pin::Pin<&mut datagram::Socket<'d, ID, frames::Datagram, send::Sourced>>,
        now: clock::Moment<'d>,
        work: app::Work<'_, 'd>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        self.get_mut().send(sock, now, work, token);
    }

    fn progress(
        self: pin::Pin<&Self>,
        _: clock::Clock<'_, 'd>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        if self.db.queries.has_udp(token, self.pool.available()) {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        }
    }
}
