use std::{cell::Cell, net::SocketAddr};

use dope_core::driver::{self, settings, storage};
use dope_manifold::{
    Bundle,
    connector::{
        Engine,
        app::{self, Application, ChunkOutcome, CloseOutcome},
        attempt::{
            self, StreamTarget,
            queue::{self, Source},
        },
        connection, lifecycle,
    },
    service::{self, Endpoint, ReconcileError, Revision, Snapshot},
    timing::Throughput,
};
use dope_net::{link::egress, tcp::Tcp, wire::Identity};
use dope_runtime::executor::Executor;
use dope_test::checks::TrackingAlloc;
use o3::{
    buffer::{bytes::Retainable, storage::Shared},
    collections::slab::Capacity,
};

#[global_allocator]
static ALLOCATOR: TrackingAlloc = TrackingAlloc::new();

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

struct SourceFactory(Capacity);

impl storage::Factory for SourceFactory {
    type Output<'d> = Source<'d, Tcp>;
    type Error = std::io::Error;

    fn build<'d>(
        self,
        context: &mut storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'d>, Self::Error> {
        Source::with_capacity(self.0, context)
    }
}

fn with_source<R>(
    capacity: Capacity,
    run: impl for<'d> FnOnce(&Source<'d, Tcp>, &mut driver::Context<'_, 'd>) -> R,
) -> R {
    let config =
        settings::Config::for_tcp_profile::<Throughput>(2).expect("attempt source driver config");
    let entered = Executor::new(config)
        .expect("attempt source executor")
        .with_factory(SourceFactory(capacity))
        .try_enter(|mut session| run(session.storage(), &mut session.driver_access()));
    match entered {
        Ok(output) => output,
        Err(error) => panic!("source factory failed: {error}"),
    }
}

#[test]
fn controller_is_a_zero_overhead_borrowed_view() {
    assert_eq!(
        std::mem::size_of::<queue::Control<'static, 'static, Tcp>>(),
        std::mem::size_of::<&'static Source<'static, Tcp>>()
    );
    assert_eq!(
        std::mem::size_of::<queue::Lease<'static, 'static, Tcp>>(),
        2 * std::mem::size_of::<usize>()
    );
}

#[test]
fn dropping_a_queued_lease_cancels_and_releases_its_generation() {
    with_source(Capacity::new(1), |queue, _driver| {
        let lease = queue
            .dial(StreamTarget::new(addr(8995), Default::default()))
            .expect("leased attempt");
        let key = lease.id();

        let ((), allocation) = TrackingAlloc::<0>::measure(|| drop(lease));
        assert_eq!(allocation, (0, 0));

        let replacement = queue
            .dial(StreamTarget::new(addr(8996), Default::default()))
            .expect("dropped owner released capacity");
        let replacement = replacement.id();
        assert_eq!(replacement.index(), key.index());
        assert_ne!(replacement, key);
    });
}

struct EmptyApp;

impl<'d> Application<'d> for EmptyApp {
    type Conn = ();
    type Wire = Identity;
    type Send = Shared;
    type Input = dope_manifold::receive::Borrowed;

    fn connection(&self) -> Self::Conn {}
}

impl<'d> app::Receive<'d> for EmptyApp {
    type Continuation = app::continuation::Complete;
}

impl<'d> app::BorrowedReceive<'d> for EmptyApp {
    fn chunk<O, R: Retainable>(
        &mut self,
        _connection: connection::Ctx<'_, 'd, 0, Identity, (), O>,
        _egress: egress::Queue<'_, 'd, 32>,
        _chunk: R,
        _driver: &mut driver::Context<'_, 'd>,
    ) -> ChunkOutcome {
        ChunkOutcome::Ok
    }
}

impl<'d> app::Lifecycle<'d> for EmptyApp {
    fn connected<O>(
        &mut self,
        _key: attempt::Id<'d>,
        _peer: dope_core::io::socket::Addr,
        _connection: connection::Ctx<'_, 'd, 0, Identity, (), O>,
        _egress: egress::Queue<'_, 'd, 32>,
        _driver: &mut driver::Context<'_, 'd>,
    ) {
    }

    fn sent(&mut self, _connection: connection::Id<'d, 0>, _has_pending_egress: bool) {}

    fn close<O>(
        &mut self,
        _connection: connection::Ctx<'_, 'd, 0, Identity, (), O>,
        _egress: egress::Queue<'_, 'd, 32>,
        reason: lifecycle::CloseReason,
        _driver: &mut driver::Context<'_, 'd>,
    ) -> CloseOutcome {
        CloseOutcome::Complete(reason)
    }
}

impl<'d> app::RequestSource<'d> for EmptyApp {
    fn drain_requests(
        &self,
        _connection: connection::Id<'d, 0>,
        _state: &mut Self::Conn,
        _drain: &mut app::RequestDrain<'_, 'd, Shared>,
        _driver: &mut driver::Context<'_, 'd>,
    ) -> app::Requests {
        app::Requests::default()
    }
}

impl<'d> app::Scheduling<'d> for EmptyApp {
    fn pre_park<'turn>(
        &mut self,
        _work: driver::schedule::Application<'turn, 'd>,
        _region: &mut o3::cell::region::Token<'d>,
    ) {
        let _ = self;
    }

    fn shutdown(&mut self) {
        let _ = self;
    }

    fn progress(
        &self,
        _region: &o3::cell::region::Token<'d>,
    ) -> dope_core::driver::schedule::Progress<'d> {
        dope_core::driver::schedule::Progress::Quiescent
    }
}

#[test]
fn engine_reports_attempt_capacity_mismatch_without_panicking() {
    with_source(Capacity::new(1), |source, driver| {
        let error = Engine::<0, _, _, Bundle<Tcp, Identity, Throughput>>::with_attempt_source(
            EmptyApp,
            source,
            2,
            Default::default(),
            (),
            driver,
        )
        .err()
        .expect("capacity mismatch");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "connector controller capacity 1 is below requested 2"
        );
    });
}

#[test]
fn reconciliation_policy_has_no_runtime_storage_cost() {
    assert_eq!(
        std::mem::size_of::<service::reconcile::Preserve>(),
        std::mem::size_of::<service::reconcile::Replace>()
    );
    assert_eq!(
        std::mem::align_of::<service::reconcile::Preserve>(),
        std::mem::align_of::<service::reconcile::Replace>()
    );
    assert_eq!(std::mem::size_of::<service::reconcile::Preserve>(), 0);
    assert_eq!(std::mem::size_of::<service::reconcile::Replace>(), 0);
}

#[test]
fn snapshot_rejects_more_than_its_type_level_endpoint_limit() {
    let endpoints = [
        Endpoint::new(addr(9601), addr(9601)),
        Endpoint::new(addr(9602), addr(9602)),
    ];
    let error = Snapshot::<_, 1>::try_new(Revision::new(1), endpoints).err();

    assert_eq!(error, Some(ReconcileError::Capacity { limit: 1 }));
}

#[test]
fn snapshot_accepts_the_absolute_endpoint_limit() {
    let endpoints = (0..service::MAX_ENDPOINTS).map(|index| {
        let port = 10_000 + u16::try_from(index).expect("bounded endpoint index");
        let endpoint = SocketAddr::from(([127, 0, 0, 1], port));
        Endpoint::new(endpoint, endpoint)
    });

    let snapshot = Snapshot::<_, { service::MAX_ENDPOINTS }>::try_new(Revision::new(1), endpoints)
        .expect("absolute endpoint limit");

    assert_eq!(snapshot.endpoints().len(), service::MAX_ENDPOINTS);
}

#[test]
fn snapshot_construction_allocates_nothing() {
    let first = SocketAddr::from(([127, 0, 0, 1], 10_016));
    let second = SocketAddr::from(([127, 0, 0, 1], 10_017));

    let (snapshot, allocation) = TrackingAlloc::<0>::measure(|| {
        Snapshot::<_, 2>::try_new(
            Revision::new(1),
            [Endpoint::new(first, first), Endpoint::new(second, second)],
        )
        .expect("bounded snapshot")
    });

    assert_eq!(allocation, (0, 0));
    assert_eq!(snapshot.endpoints().len(), 2);
}

#[test]
fn snapshot_capacity_failure_is_bounded_and_drops_every_produced_value() {
    struct Counted<'a>(&'a Cell<usize>);

    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    let produced = Cell::new(0);
    let dropped = Cell::new(0);
    let endpoints = std::iter::from_fn(|| {
        produced.set(produced.get() + 1);
        Some(Counted(&dropped))
    });

    let error = Snapshot::<_, 2>::try_new(Revision::new(1), endpoints).err();

    assert_eq!(error, Some(ReconcileError::Capacity { limit: 2 }));
    assert_eq!(produced.get(), 3);
    assert_eq!(dropped.get(), 3);
}
