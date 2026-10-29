pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io,
    net::{SocketAddr, TcpListener},
    rc::Rc,
    time::Duration,
};

use dope_core::{
    driver::{
        life::recovery,
        route::{Id, KeyTag},
    },
    platform::wake,
};
use dope_manifold::{
    app::{Host, env::Bundle},
    client::session::Setup,
    network::client::topology::plan::{Plan, Spec},
    service::{
        self, Endpoint, Revision, Snapshot,
        health::Domain,
        sync::{Preserve, Replace},
    },
    timing::Balanced,
};
use dope_net::{
    tcp::Tcp,
    wire::{Identity, domain::Local},
};
use dope_runtime::executor::{
    Executor,
    session::{Completion, Scope},
};
use dope_test::{
    alloc::Tracker,
    harness::{
        GUARD,
        observe::Trace,
        peer::AcceptScript,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use crate::common::{
    contract::{Report, codec},
    manifold::client::{
        Configure as _,
        connect::resolve::deferred_revision,
        finish::close::{
            members::{Notice, Source, Until, Watch},
            observe::{Events, Session},
        },
    },
};

/// Withdrawal preserves the endpoint owned by an already selected epoch.
#[test]
fn withdrawn_epoch_connects() {
    deferred_revision::<Preserve>(None);
}

/// Authoritative discovery rebinds a deferred plan when only its address changes.
#[test]
fn updated_deferred_epoch_connects() {
    let prior = TcpListener::bind("127.0.0.1:0").expect("prior endpoint");
    deferred_revision::<Replace>(Some(prior.local_addr().expect("prior address")));
}

/// Replacement and withdrawal remove pending retries before the same slot is reused.
#[test]
fn retired_retries() {
    let mut finish = Completion::new();
    let (close_rx, close_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    let (end_rx, end_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    let refused = TcpListener::bind("127.0.0.1:0").expect("reserve refused address");
    let bad = refused.local_addr().expect("refused address");
    AcceptScript::with(move |acceptor| {
                let first = acceptor.accept()?;
                close_rx
                    .wait_timeout(GUARD)
                    .expect("close connected endpoint");
                drop(first);
                let second = acceptor.accept()?;
                end_rx.wait_timeout(GUARD).expect("release reused endpoint");
                drop(second);
                Ok::<_, io::Error>(())
            }, |server| {
        drop(refused);
        let good = server.addr();
        let pending = Rc::new(Cell::new(Some(
            Snapshot::<Endpoint<u32, SocketAddr>, 1>::from_one(
                Revision::new(1),
                Endpoint::new(10, bad),
            ),
        )));
        let failures = Gate::new();
        let opened = Gate::new();
        let changes = Rc::new(Trace::new());
        let plan = Plan::<KeyTag<0>, 1>::new(
            1,
            Spec::new(Duration::from_secs(2), Duration::from_secs(4)).expect("valid retry bounds"),
        )
        .expect("single connection");
        let config = plan.configured().expect("discovery resources");
        use dope_core::driver::life::creation::Attempt;
        let mut create = Attempt::new();
        let result = Executor::new(&mut create, config);
        result.as_ref().map_err(|error| error.kind()).expect("executor construction");
        let executor = result.ok().expect("executor construction");
        let mut reclaim2 = Reports::<_>::new(config, [Id::fixed::<0>()]);

        executor.with_storage(()).run(|executor| {
            let result = executor.enter_with(
                |mut runtime| {
                    use dope_manifold::client::session::ingress::Plan;

                    let events = Rc::new(Cell::new(Events::default()));
                    let errors = Rc::new(Trace::new());
                    let audit = Rc::new(Report::default());
                    let target = Session {
                        audit: audit.clone(),
                        codec: codec::Outbound,
                        errors: errors.clone(),
                        events: events.clone(),
                        gate: opened.clone(),
                    };
                    let hash = runtime.hash_state(Domain::DEFAULT);
                    let result = plan.bind(Local::new(Identity), hash).discover::<_, _, _, Preserve, _, _, Bundle<Tcp, Identity, Balanced>, 1, _, _>(
                        Setup::new(target, Plan::none()),
                        Source {
                            pending: pending.clone(),
                            failures: failures.clone(),
                        },
                        Watch(changes.clone()),
                        &mut runtime,
                        Scope::driver,
                        |host, runtime| {
                            let now = runtime.driver().driver_ref().clock().now();
                            let timer = runtime.driver().driver_ref().timer();
                            let result = runtime.with_at_for(&mut finish,
                                Host::new(host),
                                now.into(),
                                Duration::from_secs(2),
                                |mut app| {
                                    let at = |ms| now.clamp_add(Duration::from_millis(ms));
                                    app.drive(at(0), Until(|| failures.hits() == 1))
                                        .expect("refused endpoint enters backoff")
                                        .into_output();
                                    app.drive(at(2000), Until(|| failures.hits() == 2))
                                        .expect("same endpoint increases backoff")
                                        .into_output();
                                    let later = timer.earliest().expect("second backoff is armed");
                                    assert!(at(4000) <= later && later < at(6000));
                                    pending.set(Some(Snapshot::<Endpoint<u32, SocketAddr>, 1>::from_one(
                                            Revision::new(2),
                                            Endpoint::new(11, bad),
                                        )));
                                    app.drive(at(2000), Until(|| failures.hits() == 3))
                                        .expect("new endpoint starts its own backoff")
                                        .into_output();
                                    let earlier = timer.earliest().expect("replacement backoff is armed");
                                    assert!(at(3000) <= earlier && earlier < at(4000));
                                    pending.set(Some(Snapshot::<Endpoint<u32, SocketAddr>, 1>::from_one(
                                            Revision::new(3),
                                            Endpoint::new(20, good),
                                        )));
                                    app.drive(at(2001), Until(|| opened.hits() == 1))
                                        .expect("replacement connects before old retry")
                                        .into_output();
                                    close_tx.notify().expect("fail replacement endpoint");
                                    app.drive(at(2002), Until(|| failures.hits() == 4))
                                        .expect("replacement enters backoff")
                                        .into_output();
                                    pending.set(Some(Snapshot::<Endpoint<u32, SocketAddr>, 1>::empty(
                                            Revision::new(4),
                                        )));
                                    app.drive(at(2003), Until(|| changes.values().expect("discovery observations").len() == 8))
                                        .expect("withdraw backed-off endpoint")
                                        .into_output();
                                    app.drive(at(5000), Until(|| true))
                                        .expect("removed retry deadlines are quiescent")
                                        .into_output();
                                    assert_eq!(failures.hits(), 4);
                                    assert_eq!(opened.hits(), 1);
                                    pending.set(Some(Snapshot::<Endpoint<u32, SocketAddr>, 1>::from_one(
                                            Revision::new(5),
                                            Endpoint::new(30, good),
                                        )));
                                    app.drive(at(5001), Until(|| opened.hits() == 2))
                                        .expect("withdrawn slot accepts a fresh endpoint")
                                        .into_output();
                                    assert_eq!(failures.hits(), 4);
                                    assert!(
                                        Events::snapshot(&events)
                                            .peers
                                            .iter()
                                            .copied()
                                            .map(|peer| peer.into_std().expect("TCP peer address"))
                                            .eq([good, good])
                                    );
                                    assert_eq!(Events::snapshot(&events).closed.len(), 1);
                                    pending.set(Some(Snapshot::<Endpoint<u32, SocketAddr>, 1>::empty(
                                            Revision::new(6),
                                        )));
                                    app.drive(at(5002), Until(|| changes.values().expect("discovery observations").len() == 12))
                                        .expect("withdraw connected endpoint").into_output();
                                    end_tx.notify().expect("fail retired endpoint");
                                    app.drive(at(5003), Until(|| {
                                        let observed = events.take();
                                        let closed = observed.closed.len();
                                        events.set(observed);
                                        closed == 2
                                    }))
                                        .expect("retired epoch closes without health feedback").into_output();
                                    pending.set(Some(Snapshot::<Endpoint<u32, SocketAddr>, 1>::empty(
                                            Revision::new(7),
                                        )));
                                    app.drive(at(5004), Until(|| changes.values().expect("discovery observations").len() == 14))
                                        .expect("observe discovery after retired failure").into_output();
                                    assert_eq!(failures.hits(), 4);
                                },
                            );
                            assert!(result.is_ok(), "discovery application teardown");
                        },
                    );
                    assert!(result.is_ok(), "install discovery connector");
                    audit.checked((0, 0));
                    assert!(errors.values().expect("codec observations").is_empty());
                    let observed = Events::snapshot(&events);
                    assert_eq!(opened.hits() as usize, observed.conn.len());
                    assert!(observed.replies.is_some(), "reply count overflowed");
                    assert!(observed.is_ordered(), "{:?}", (&observed.conn, &observed.closed, &observed.quarantined));
                },
                recovery::Policy::new(|event| reclaim2.push(event), recover),
            );
            assert!(
                matches!(reclaim2.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim2:?}"
            );

            assert!(result.is_ok(), "discovery executor teardown");
            let changes = changes.values().expect("discovery observations");
            assert_eq!(changes.len(), 14);
            for (index, id) in [Some(10), Some(11), Some(20), None, Some(30), None, None].into_iter().enumerate() {
                let rev = Revision::new(index as u64 + 1);
                assert!(changes[index * 2] == Notice::Resolved(rev, id));
                assert!(
                    changes[index * 2 + 1]
                        == Notice::Reconciled(
                            rev,
                            service::Change {
                                retained: 0,
                                added: usize::from(id.is_some()),
                                retired: usize::from(index == 1 || index == 2 || index == 3 || index == 5),
                            },
                        )
                );
            }
            assert!(server.finish().expect("join discovery peer").is_ok());
        });
    }).expect("bind discovery endpoint");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
