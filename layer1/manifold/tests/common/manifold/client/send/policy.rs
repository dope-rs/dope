use std::{cell::Cell, convert::Infallible};

use dope_core::{
    Errno, driver,
    driver::{route::KeyTag, schedule::app::Work, schedule::ready},
    io::socket,
};
use dope_manifold::{
    client::{
        app::{self, admit::Automatic, connect::Full, failure::Observed, request},
        attempt, close, conn,
        session::{self, app::drain::Ready, codec::mode, policy, protocol, response, turns},
    },
    service::bindings::endpoint::State,
};
use dope_net::link::{egress, event, pool::change::opening};
use dope_test::harness::signals::{Gate, Progress};
use o3::{
    base::{Call, cell::region},
    buffer::{self, hold, storage::Shared},
};

use crate::common::contract::{codec, codec::errors};

type Owner<'life, 'd, const TAKE: bool, const REPORT: bool = true> =
    State<'life, 'd, Probe<'d>, Logic<TAKE, REPORT>, KeyTag<0>, 1>;
type Context<'a, 'e, 'd> = turns::Context<'a, 'e, 'd, Option<ready::Target<'d>>, Shared>;
type Retired<'a, 'life, 'd, const TAKE: bool, const REPORT: bool = true> =
    turns::Retired<'a, 'd, Option<ready::Target<'d>>, KeyTag<0>, Owner<'life, 'd, TAKE, REPORT>>;

#[derive(Default)]
pub struct Probe<'d> {
    pub(crate) eof: Gate,
    pub(crate) release: Gate,
    pub(crate) closed: Gate,
    connection: Cell<Option<conn::Id<'d, KeyTag<0>>>>,
    target: Cell<Option<ready::Target<'d>>>,
    failure: Cell<Option<conn::Transmit>>,
    offered: Cell<u32>,
    quarantined: Cell<u32>,
    cleared: Cell<u32>,
}

impl<'d> Probe<'d> {
    pub fn send(&self) {
        self.release.hit();
        self.target
            .get()
            .expect("connected readiness target")
            .wake();
    }

    pub fn check<const REPORT: bool>(&self) {
        assert_eq!(self.offered.get(), u32::from(REPORT));
        assert_eq!(self.quarantined.get(), 1);
        assert_eq!(self.cleared.get(), 3, "retirement retries then waits");
        assert_eq!(self.closed.hits(), 1);
        check_failure(self.failure.get().expect("exact send failure"));
    }
}

impl Progress for Probe<'_> {
    fn hits(&self) -> u32 {
        u32::from(self.connection.get().is_some())
    }
}

fn check_failure(failure: conn::Transmit) {
    assert!(
        matches!(failure,
            conn::Transmit::Io(event::SendFailure::Queued(error))
                if error.is_pipe() || error.is_reset()
        ),
        "unexpected native send failure: {failure:?}",
    );
}

struct Logic<const TAKE: bool, const REPORT: bool = true> {
    pending: Option<Shared>,
    sent: bool,
}

impl<const TAKE: bool, const REPORT: bool> Logic<TAKE, REPORT> {
    fn new() -> Self {
        Self {
            pending: Some(Shared::try_copy(b"x").expect("send payload")),
            sent: false,
        }
    }

    fn fence(&self, state: &Option<ready::Target<'_>>) -> app::Fence {
        if let (Some(target), false) = (state, self.sent) {
            target.wake();
            app::Fence::Pending
        } else {
            app::Fence::Ready
        }
    }

    fn quarantine<'life, 'd>(
        owner: &Owner<'life, 'd, TAKE, REPORT>,
        id: conn::Id<'d, KeyTag<0>>,
        reason: close::Reason,
    ) {
        let probe = owner.root();
        assert_eq!(
            probe.connection.get(),
            Some(id),
            "exact connection generation"
        );
        assert_eq!(reason, close::Reason::Transport);
        assert_eq!(probe.quarantined.replace(1), 0);
        assert_eq!(
            probe.offered.get(),
            u32::from(REPORT),
            "offer precedes quarantine",
        );
        assert_eq!(probe.failure.get().is_some(), REPORT);
    }

    fn clear_recv<'life, 'd>(
        owner: &Owner<'life, 'd, TAKE, REPORT>,
        id: conn::Id<'d, KeyTag<0>>,
    ) -> egress::Cleared {
        let probe = owner.root();
        assert_eq!(probe.connection.get(), Some(id));
        assert_eq!(probe.quarantined.get(), 1);
        let cleared = probe.cleared.get();
        probe.cleared.set(cleared + 1);
        match cleared {
            0 => egress::Cleared::Retry,
            1 => {
                probe
                    .target
                    .get()
                    .expect("retirement readiness target")
                    .wake();
                egress::Cleared::Waiting
            }
            _ => egress::Cleared::Done,
        }
    }

    fn disconnect<'life, 'd>(
        owner: &Owner<'life, 'd, TAKE, REPORT>,
        mut context: Retired<'_, 'life, 'd, TAKE, REPORT>,
    ) {
        let probe = owner.root();
        assert_eq!(probe.connection.get(), Some(context.connection()));
        assert_eq!(context.reason(), close::Reason::Transport);
        assert_eq!(probe.cleared.get(), 3);
        let observed = context.transmit().copied();
        let taken = context.take_transmit();
        assert_eq!(taken, observed, "taking preserves the exact diagnostic");
        if TAKE {
            assert!(taken.is_none(), "callback already owns the diagnostic");
            assert_eq!(
                probe.offered.get(),
                1,
                "retry does not reoffer a consumed cause",
            );
        } else {
            let failure = taken.expect("default lifecycle retains the diagnostic");
            check_failure(failure);
            assert_eq!(
                probe.failure.replace(Some(failure)),
                REPORT.then_some(failure)
            );
        }
        assert!(context.transmit().is_none());
        assert!(context.take_transmit().is_none());
        assert_eq!(probe.closed.hits(), 0);
        probe.closed.hit();
    }
}

impl<'life, 'd: 'life, const TAKE: bool, const REPORT: bool>
    protocol::Spec<'life, 'd, Probe<'d>, KeyTag<0>, 1> for Logic<TAKE, REPORT>
{
    type Codec = codec::Pending;
    type Mode = mode::Plain;
    type Requests = request::Disabled;
    type Response = response::Immediate;
    type Connect = Full;
    type ConnState = Option<ready::Target<'d>>;
    type Failure = Observed;
    type Send = Shared;

    fn codec<'a>(_: &'a Owner<'life, 'd, TAKE, REPORT>) -> &'a Self::Codec {
        &codec::Pending
    }

    fn mode(
        _: &Owner<'life, 'd, TAKE, REPORT>,
        _: conn::Id<'d, KeyTag<0>>,
        _: &Self::ConnState,
    ) -> session::ReceiveMode {
        session::ReceiveMode::Duplex
    }

    fn eof<'e>(
        owner: &mut Owner<'life, 'd, TAKE, REPORT>,
        context: &mut Context<'_, 'e, 'd>,
    ) -> app::Eof {
        let target = context.wake_target();
        *context.state = Some(target);
        owner.root().eof.hit();
        target.wake();
        app::Eof::Retain
    }
}

impl<'life, 'd: 'life, const TAKE: bool, const REPORT: bool>
    protocol::Plain<'life, 'd, Probe<'d>, KeyTag<0>, 1> for Logic<TAKE, REPORT>
{
    fn deliver<'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        _: &mut Owner<'life, 'd, TAKE, REPORT>,
        head: Infallible,
        _: &mut Context<'_, 'e, 'd>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        match head {}
    }

    fn reject<'e>(
        _: &mut Owner<'life, 'd, TAKE, REPORT>,
        error: errors::Truncated,
        _: &mut Context<'_, 'e, 'd>,
    ) -> response::Direct {
        panic!("unexpected protocol input: {error:?}");
    }
}

impl<'life, 'd: 'life, const TAKE: bool, const REPORT: bool>
    policy::Output<'life, 'd, Probe<'d>, KeyTag<0>, 1> for Logic<TAKE, REPORT>
{
    fn flush() -> Option<
        impl for<'a, 'c, 't, 'e> Call<(
            &'a mut Owner<'life, 'd, TAKE, REPORT>,
            &'c mut Context<'t, 'e, 'd>,
            session::Input,
        )>,
    > {
        Some(
            |(owner, context, _): (
                &mut Owner<'life, 'd, TAKE, REPORT>,
                &mut Context<'_, '_, 'd>,
                session::Input,
            )| {
                if owner.root().eof.hits() == 0 || owner.root().release.hits() == 0 {
                    return;
                }
                let Some(payload) = owner.logic_mut().pending.take() else {
                    return;
                };
                let mut parts = context.parts();
                assert!(matches!(
                    parts.sink.try_enqueue(parts.driver.region_token(), payload),
                    egress::Enqueue::Committed
                ));
            },
        )
    }

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Owner<'life, 'd, TAKE, REPORT>,
        conn::Id<'d, KeyTag<0>>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        |(owner, _, pending, _): (
            &mut Owner<'life, 'd, TAKE, REPORT>,
            conn::Id<'d, KeyTag<0>>,
            egress::Pending,
            &mut region::Token<region::Identity<'d, 1>>,
        )| {
            assert!(matches!(pending, egress::Pending::Absent));
            owner.logic_mut().sent = true;
        }
    }
}

impl<'life, 'd: 'life, const TAKE: bool, const REPORT: bool>
    policy::Connected<'life, 'd, Probe<'d>, KeyTag<0>, 1> for Logic<TAKE, REPORT>
{
    type Activation = Automatic;

    fn connect<'e>(
        owner: &mut Owner<'life, 'd, TAKE, REPORT>,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        context: turns::Startup<'_, 'e, 'd, Self::ConnState, Self::Send>,
    ) {
        let probe = owner.root();
        assert!(
            probe
                .connection
                .replace(Some(context.connection()))
                .is_none()
        );
        probe.target.set(Some(context.ready));
    }
}

impl<'life, 'd: 'life, const TAKE: bool, const REPORT: bool>
    policy::Failed<'life, 'd, Probe<'d>, KeyTag<0>, 1> for Logic<TAKE, REPORT>
{
    fn failed(
        _: &mut Owner<'life, 'd, TAKE, REPORT>,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        panic!("unexpected connect failure: {error:?}");
    }
}

impl<'life, 'd: 'life, const TAKE: bool, const REPORT: bool>
    policy::Exhausted<'life, 'd, Probe<'d>, KeyTag<0>, 1> for Logic<TAKE, REPORT>
{
    fn exhaust(
        _: &mut Owner<'life, 'd, TAKE, REPORT>,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        panic!("unexpected admission failure: {error:?}");
    }
}

impl<'life, 'd: 'life, const TAKE: bool, const REPORT: bool>
    policy::Scheduling<'life, 'd, Probe<'d>, KeyTag<0>, 1> for Logic<TAKE, REPORT>
{
    fn park<'turn>(
        _: &mut Owner<'life, 'd, TAKE, REPORT>,
        _: Work<'turn, 'd>,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
    }

    fn shutdown(_: &mut Owner<'life, 'd, TAKE, REPORT>) {}
}

impl<'life, 'd: 'life, const TAKE: bool> policy::Lifecycle<'life, 'd, Probe<'d>, KeyTag<0>, 1>
    for Logic<TAKE>
{
    type Drain = Ready;

    fn transmit_failed(
        owner: &Owner<'life, 'd, TAKE>,
        id: conn::Id<'d, KeyTag<0>>,
        failure: conn::Transmit,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Option<conn::Transmit> {
        let probe = owner.root();
        assert_eq!(probe.connection.get(), Some(id));
        assert_eq!(
            probe.quarantined.get(),
            0,
            "exact error arrives before quarantine",
        );
        assert_eq!(probe.offered.replace(1), 0);
        check_failure(failure);
        assert!(probe.failure.replace(Some(failure)).is_none());
        (!TAKE).then_some(failure)
    }

    fn io_fail<'e>(_: &mut Owner<'life, 'd, TAKE>, error: Errno, _: &mut Context<'_, 'e, 'd>) {
        panic!("unexpected receive failure: {error:?}");
    }

    fn quarantine(
        owner: &Owner<'life, 'd, TAKE>,
        id: conn::Id<'d, KeyTag<0>>,
        _: &mut Self::ConnState,
        reason: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        Self::quarantine(owner, id, reason);
    }

    fn clear_recv<'turn>(
        owner: &Owner<'life, 'd, TAKE>,
        id: conn::Id<'d, KeyTag<0>>,
        _: close::Reason,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        Self::clear_recv(owner, id)
    }

    fn fence(
        owner: &Owner<'life, 'd, TAKE>,
        _: conn::Id<'d, KeyTag<0>>,
        state: &Self::ConnState,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        owner.logic().fence(state)
    }

    fn policy(
        _: &Owner<'life, 'd, TAKE>,
        _: conn::Id<'d, KeyTag<0>>,
        _: &Self::ConnState,
        _: close::Reason,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> close::Policy {
        close::Policy::Permanent
    }

    fn disconnect(owner: &mut Owner<'life, 'd, TAKE>, context: Retired<'_, 'life, 'd, TAKE>) {
        Self::disconnect(owner, context);
    }
}

impl<'life, 'd: 'life> policy::Lifecycle<'life, 'd, Probe<'d>, KeyTag<0>, 1>
    for Logic<false, false>
{
    type Drain = Ready;

    fn io_fail<'e>(
        _: &mut Owner<'life, 'd, false, false>,
        error: Errno,
        _: &mut Context<'_, 'e, 'd>,
    ) {
        panic!("unexpected receive failure: {error:?}");
    }

    fn quarantine(
        owner: &Owner<'life, 'd, false, false>,
        id: conn::Id<'d, KeyTag<0>>,
        _: &mut Self::ConnState,
        reason: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        Self::quarantine(owner, id, reason);
    }

    fn clear_recv<'turn>(
        owner: &Owner<'life, 'd, false, false>,
        id: conn::Id<'d, KeyTag<0>>,
        _: close::Reason,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        Self::clear_recv(owner, id)
    }

    fn fence(
        owner: &Owner<'life, 'd, false, false>,
        _: conn::Id<'d, KeyTag<0>>,
        state: &Self::ConnState,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        owner.logic().fence(state)
    }

    fn policy(
        _: &Owner<'life, 'd, false, false>,
        _: conn::Id<'d, KeyTag<0>>,
        _: &Self::ConnState,
        _: close::Reason,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> close::Policy {
        close::Policy::Permanent
    }

    fn disconnect(
        owner: &mut Owner<'life, 'd, false, false>,
        context: Retired<'_, 'life, 'd, false, false>,
    ) {
        Self::disconnect(owner, context);
    }
}

pub fn check_transmit<const TAKE: bool, const REPORT: bool>() {
    use std::{net, time::Duration};

    use dope_core::driver::{life::recovery, route::Id};
    use dope_manifold::{
        app::{Host, env::Bundle},
        client::session::ingress,
        network::client::topology::plan,
        service::{Endpoint, Revision, Snapshot, bindings::Bound, health, sync},
    };
    use dope_net::{
        tcp::Tcp,
        wire::{Identity, domain},
    };
    use dope_runtime::executor::session::{Completion, Scope};
    use dope_test::{
        fibers,
        harness::{peer, scenario},
        reclaim::Reports,
    };

    use super::profile;
    use crate::common::{
        recover,
        scenario::{Runtime, RuntimeCase},
    };

    let listener =
        peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("reset listener");
    let address = listener.local_addr().expect("listener address");
    let mut finish = Completion::new();
    let plan = plan::Plan::<KeyTag<0>, 1>::new(
        1,
        plan::Spec::new(Duration::from_secs(1), Duration::from_secs(1)).expect("retry bounds"),
    )
    .expect("client layout");
    let runtime = Runtime::try_for_plan::<profile::Slow, _>(&plan).expect("client settings");
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);
    runtime.with_scope(
        |mut runtime| {
            let probe = Probe::default();
            let hash = runtime.hash_state(health::Domain::DEFAULT);
            let peers = Snapshot::from_one(Revision::new(1), Endpoint::new(address, address));
            let run = |app: &mut dyn RuntimeCase| {
                assert_eq!(
                    app.try_wait(&probe, 1).expect("client connected"),
                    fibers::Wait::Reached,
                );
                let peer = listener
                    .accept_for(scenario::TIMEOUT)
                    .expect("accept peer")
                    .expect("connected peer");
                peer.shutdown(net::Shutdown::Write).expect("receive EOF");
                assert_eq!(
                    app.try_wait(&probe.eof, 1).expect("retained EOF"),
                    fibers::Wait::Reached,
                );
                peer::Peer::try_reset(&peer).expect("reset on close");
                drop(peer);
                probe.send();
                assert_eq!(
                    app.try_wait(&probe.closed, 1)
                        .expect("send failure retirement"),
                    fibers::Wait::Reached,
                );
            };
            let result = if REPORT {
                let session = Bound::<_, _, KeyTag<0>, 1>::bind(&probe, Logic::<TAKE>::new());
                let setup = session::Setup::new(session, ingress::Plan::none());
                plan.bind(domain::Local::new(Identity), hash).install::<
                            _, _, sync::Preserve, _, Bundle<Tcp, Identity, profile::Slow>, 1, _, _,
                        >(
                            setup,
                            peers,
                            &mut runtime,
                            Scope::driver,
                            |host, runtime| {
                                runtime
                                    .with_quiesce(
                                        &mut finish,
                                        Host::new(host),
                                        scenario::TIMEOUT,
                                        |mut app| run(&mut app),
                                    )
                                    .expect("bound client lifecycle");
                            },
                        )
            } else {
                let session =
                    Bound::<_, _, KeyTag<0>, 1>::bind(&probe, Logic::<false, false>::new());
                let setup = session::Setup::new(session, ingress::Plan::none());
                plan.bind(domain::Local::new(Identity), hash).install::<
                            _, _, sync::Preserve, _, Bundle<Tcp, Identity, profile::Slow>, 1, _, _,
                        >(
                            setup,
                            peers,
                            &mut runtime,
                            Scope::driver,
                            |host, runtime| {
                                runtime
                                    .with_quiesce(
                                        &mut finish,
                                        Host::new(host),
                                        scenario::TIMEOUT,
                                        |mut app| run(&mut app),
                                    )
                                    .expect("bound client lifecycle");
                            },
                        )
            };
            assert!(result.is_ok(), "bound client installation");
            probe.check::<REPORT>();
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(reports.events().expect("reclaimed events").is_empty());
}
