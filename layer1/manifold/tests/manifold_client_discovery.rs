pub mod common;

use crate::common::manifold::client::{assert_reclaimed};

use std::{
    cell::Cell,
    io::Write as _,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    num::NonZero,
    rc::Rc,
    time::Duration,
};

use dope_core::driver::{
    life::recovery,
    route::{Id, KeyTag},
};
use dope_manifold::{
    app::{Host, env::Bundle},
    client::{close::Reason, session::Setup},
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
    drive,
    session::{Completion, Scope},
};
use dope_test::{
    alloc::Tracker,
    harness::{
        GUARD,
        observe::Trace,
        peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::{base::time::Positive, depot::array};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use crate::common::{
    contract::codec,
    manifold::client::{
        Configure as _,
        connect::resolve::deferred_revision,
        finish::close::{
            members::{
                Address, Admission, Demand, DemandState, LimitedDemand, Net, Notice, Renewal,
                Source, Until, Watch,
            },
            observe::{Events, Session},
        },
        side::limited::Limited,
        switch::terminal::{plaintext::Decode, queued},
    },
    recover,
    scenario::Runtime,
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

/// New snapshots cannot restart a budgeted retirement scan ahead of a live slot.
#[test]
fn repeated_replacement_retires_every_connection() {
    run::<Replace>();
    run::<Preserve>();

    fn run<R: service::sync::Policy>() {
        let listeners = [0; 3].map(|_| TcpListener::bind("127.0.0.1:0").expect("endpoint"));
        let addresses = listeners
            .each_ref()
            .map(|listener| listener.local_addr().expect("address"));
        let snapshot = |revision, endpoint| {
            Snapshot::<_, 2>::from_one(
                Revision::new(revision),
                Endpoint::new(endpoint, addresses[endpoint as usize]),
            )
        };
        let pending = Rc::new(Cell::new(Some(Snapshot::from_vec(
            Revision::new(1),
            array::Vec::from([0, 1].map(|id| Endpoint::new(id, addresses[id as usize]))),
        ))));
        let plan = Plan::<KeyTag<0>, 2>::new(
            2,
            Spec::new(Duration::from_secs(2), Duration::from_secs(2)).expect("retry bounds"),
        )
        .expect("two connections");
        let config = plan.configured().expect("discovery resources");
        let mut reports = Reports::<_>::new(config, [Id::fixed::<0>()]);
        let mut finish = Completion::new();
        Runtime::new(config).with_scope(
            |mut runtime| {
                let target = Session::<codec::Outbound> {
                    wait_reply: Some(Gate::new()),
                    ..Session::default()
                };
                let events = target.events.clone();
                let audit = target.audit.clone();
                let errors = target.errors.clone();
                let changes = Rc::new(Trace::new());
                let checked = Gate::new();
                let setup = Setup::new(
                    target,
                    dope_manifold::client::session::ingress::Plan::none(),
                );
                let hash = runtime.hash_state(Domain::DEFAULT);
                let installed = plan
                    .bind(Local::new(Identity), hash)
                    .discover::<_, _, _, R, _, _, Bundle<Tcp, Identity, Balanced>, 2, _, _>(
                        setup,
                        Source {
                            pending: pending.clone(),
                            failures: Gate::new(),
                        },
                        Watch(changes.clone()),
                        &mut runtime,
                        Scope::driver,
                        |host, runtime| {
                            let now = runtime.driver().driver_ref().clock().now();
                            runtime
                                .with_at_for(
                                    &mut finish,
                                    Admission {
                                        pool: Limited { inner: host },
                                        changes,
                                        checked: checked.clone(),
                                        removed: addresses[0],
                                    },
                                    now.into(),
                                    Duration::from_secs(2),
                                    |mut app| {
                                        app.drive_for(
                                            now.into(),
                                            Duration::from_secs(2),
                                            Until(|| Events::snapshot(&events).conn.len() == 2),
                                        )
                                        .expect("both original connections")
                                        .into_output();
                                        let observed = Events::snapshot(&events);
                                        let original = [observed.conn[0], observed.conn[1]];
                                        pending.set(Some(snapshot(2, 1)));
                                        app.drive_for(
                                            now.into(),
                                            Duration::from_secs(2),
                                            Until(|| {
                                                checked.hits() == 1 + u32::from(R::AUTHORITATIVE)
                                            }),
                                        )
                                        .expect("admission follows installed membership")
                                        .into_output();
                                        if !R::AUTHORITATIVE {
                                            checked.hit();
                                            return;
                                        }
                                        let retired = || {
                                            let observed = Events::snapshot(&events);
                                            original.iter().all(|id| {
                                                observed.closed.iter().any(|(closed, reason)| {
                                                    closed == id
                                                        && *reason == Reason::EndpointRetired
                                                })
                                            })
                                        };
                                        let revision = Cell::new(3);
                                        app.drive_for(
                                            now.into(),
                                            Duration::from_secs(2),
                                            Until(|| {
                                                if retired() {
                                                    return true;
                                                }
                                                let last = revision.get();
                                                let ready = pending.take();
                                                if ready.is_some() {
                                                    pending.set(ready);
                                                } else if last == 8 {
                                                    return true;
                                                } else {
                                                    pending.set(Some(snapshot(
                                                        last,
                                                        1 + (last as u32 % 2),
                                                    )));
                                                    revision.set(last + 1);
                                                }
                                                false
                                            }),
                                        )
                                        .expect("bounded replacement progress")
                                        .into_output();
                                        assert!(
                                            retired(),
                                            "every original connection retires during replacement"
                                        );
                                    },
                                )
                                .expect("application teardown");
                            if !R::AUTHORITATIVE {
                                assert_eq!(checked.hits(), 3);
                            }
                        },
                    );
                assert!(installed.is_ok(), "install connector");
                audit.checked((0, 0));
                assert!(errors.values().expect("codec observations").is_empty());
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
    }
}

/// An installed endpoint may relinquish its submitted epoch while another remains needed.
#[test]
fn withdrawn_demand_releases_single_slot() {
    for response in [None, Some(true), Some(false)] {
        let wait_reply = response.is_some();
        let first = TcpListener::bind("127.0.0.1:0").expect("first endpoint");
        let second = TcpListener::bind("127.0.0.1:0").expect("second endpoint");
        let a = first.local_addr().expect("first address");
        let b = second.local_addr().expect("second address");
        let chosen = Rc::new(Trace::new());
        let demand = Rc::<DemandState>::default();
        let mut spec =
            Spec::new(Duration::from_secs(2), Duration::from_secs(2)).expect("retry bounds");
        if response == Some(false) {
            spec = spec.with_ttl(Positive::from_millis::<250>());
        }
        let plan = Plan::<KeyTag<0>, 1>::new(1, spec).expect("one connection");
        let config = plan.configured().expect("watched connector resources");
        let mut reports = Reports::<_>::new(config, [Id::fixed::<0>()]);
        let mut finish = Completion::new();
        Runtime::new(config).with_scope(
            |mut runtime| {
                let mut target = Session::new(Decode);
                let withdrawn = Gate::new();
                target.wait_reply = wait_reply.then(|| withdrawn.clone());
                let events = target.events.clone();
                let audit = target.audit.clone();
                let errors = target.errors.clone();
                let setup = Setup::new(
                    target,
                    dope_manifold::client::session::ingress::Plan::none(),
                );
                let set = Snapshot::from_vec(
                    Revision::new(1),
                    array::Vec::from([(0, a), (1, b)].map(|(id, addr)| {
                        Endpoint::new(
                            id,
                            Address {
                                addr,
                                chosen: chosen.clone(),
                            },
                        )
                    })),
                );
                let hash = runtime.hash_state(Domain::DEFAULT);
                let installed = plan
                    .bind(Local::new(Identity), hash)
                    .watch::<_, _, _, Replace, _, Bundle<Net, Identity, Balanced>, 2, _, _>(
                        setup,
                        set,
                        Demand(demand.clone()),
                        &mut runtime,
                        Scope::driver,
                        |host, runtime| {
                            let now = runtime.driver().driver_ref().clock().now();
                            runtime
                                .with_at_for(
                                    &mut finish,
                                    Host::new(host),
                                    now.into(),
                                    Duration::from_secs(2),
                                    |mut app| {
                                        app.drive_for(
                                            now.into(),
                                            Duration::from_secs(2),
                                            Until(|| {
                                                let selected = if wait_reply {
                                                    !Events::snapshot(&events).conn.is_empty()
                                                } else {
                                                    !chosen
                                                        .values()
                                                        .expect("selected endpoints")
                                                        .is_empty()
                                                };
                                                if !selected {
                                                    return false;
                                                }
                                                demand.pending.set(Some(1));
                                                demand.live.set(Some(1));
                                                true
                                            }),
                                        )
                                        .expect("transfer submitted demand")
                                        .into_output();
                                        let _peer = if wait_reply {
                                            let (mut peer, _) =
                                                first.accept().expect("admitted peer");
                                            app.drive_for(
                                                now.into(),
                                                Duration::from_secs(2),
                                                Until(|| withdrawn.hits() != 0),
                                            )
                                            .expect("withdraw admitted generation")
                                            .into_output();
                                            assert!(Events::snapshot(&events).closed.is_empty());
                                            if response == Some(true) {
                                                peer.write_all(b"R")
                                                    .expect("complete admitted response");
                                            }
                                            Some(peer)
                                        } else {
                                            None
                                        };
                                        let next = if response == Some(false) {
                                            now.clamp_add(Duration::from_millis(250))
                                        } else {
                                            now.into()
                                        };
                                        app.drive_for(
                                            next,
                                            Duration::from_secs(2),
                                            Until(|| {
                                                Events::snapshot(&events).peers.iter().any(|peer| {
                                                    peer.into_std().expect("TCP address") == b
                                                })
                                            }),
                                        )
                                        .expect("replacement uses the released slot")
                                        .into_output();
                                        assert_eq!(
                                            Events::snapshot(&events).replies,
                                            Some(usize::from(response == Some(true)))
                                        );
                                        if wait_reply {
                                            let reason = if response == Some(true) {
                                                Reason::EndpointRetired
                                            } else {
                                                Reason::Timeout(
                                                    dope_manifold::client::deadline::Kind::Lifetime,
                                                )
                                            };
                                            let seen = Events::snapshot(&events);
                                            assert_eq!(seen.closed, vec![(seen.conn[0], reason)]);
                                        }
                                        assert_eq!(
                                            &*chosen.values().expect("selected endpoints"),
                                            &[a, b]
                                        );
                                    },
                                )
                                .expect("watched application teardown");
                        },
                    );
                assert!(installed.is_ok(), "install watched connector");
                audit.checked((0, 0));
                assert!(errors.values().expect("codec observations").is_empty());
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
    }
}

/// Native admission and queued retirement recheck live demand after its cached withdrawal.
#[test]
fn renewed_demand_preserves_submitted_and_waiting_epochs() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("endpoint");
    let address = listener.local_addr().expect("endpoint address");
    let chosen = Rc::new(Trace::new());
    let demand = Rc::<DemandState>::default();
    let plan = Plan::<KeyTag<0>, 1>::new(
        1,
        Spec::new(Duration::from_secs(2), Duration::from_secs(2)).expect("retry bounds"),
    )
    .expect("one connection");
    let config = plan.configured().expect("watched connector resources");
    let mut reports = Reports::<_>::new(config, [Id::fixed::<0>()]);
    let mut finish = Completion::new();
    Runtime::new(config).with_scope(
        |mut runtime| {
            let target = Session::<codec::Outbound>::default();
            let events = target.events.clone();
            let audit = target.audit.clone();
            let errors = target.errors.clone();
            let opened = target.gate.clone();
            let setup = Setup::new(
                target,
                dope_manifold::client::session::ingress::Plan::none(),
            );
            let set = Snapshot::<_, 1>::from_one(
                Revision::new(1),
                Endpoint::new(
                    0,
                    Address {
                        addr: address,
                        chosen: chosen.clone(),
                    },
                ),
            );
            let hash = runtime.hash_state(Domain::DEFAULT);
            let installed = plan
                .bind(Local::new(Identity), hash)
                .watch::<_, _, _, Replace, _, Bundle<Net, Identity, Balanced>, 1, _, _>(
                    setup,
                    set,
                    Demand(demand.clone()),
                    &mut runtime,
                    Scope::driver,
                    |host, runtime| {
                        let now = runtime.driver().driver_ref().clock().now();
                        runtime
                            .with_at_for(
                                &mut finish,
                                Host::new(Limited {
                                    inner: queued::Host {
                                        inner: host,
                                        state: None,
                                        renewal: Some(demand.clone()),
                                        pending: None,
                                    },
                                }),
                                now.into(),
                                Duration::from_secs(2),
                                |mut app| {
                                    app.drive_for(
                                        now.into(),
                                        Duration::from_secs(2),
                                        Until(|| opened.hits() == 1),
                                    )
                                    .expect("connect retained endpoint")
                                    .into_output();
                                    assert_eq!(
                                        &*chosen.values().expect("selected endpoints"),
                                        &[address]
                                    );
                                    demand.pending.set(None);
                                    app.drive(now.into(), Until(|| true))
                                        .expect("waiting demand becomes ineligible")
                                        .into_output();
                                    for needed in [true, false] {
                                        demand.withdrawn.set(false);
                                        demand.needed.set(needed);
                                        demand.live.set(None);
                                        app.drive_for(
                                            now.into(),
                                            Duration::from_secs(2),
                                            Until(|| {
                                                if !demand.withdrawn.get() {
                                                    return false;
                                                }
                                                demand.needed.set(true);
                                                demand.live.set(Some(0));
                                                true
                                            }),
                                        )
                                        .expect("renew demand before its queued retirement")
                                        .into_output();
                                        assert_eq!(opened.hits(), 1);
                                        assert!(Events::snapshot(&events).closed.is_empty());
                                    }
                                },
                            )
                            .expect("watched application teardown");
                    },
                );
            assert!(installed.is_ok(), "install watched connector");
            audit.checked((0, 0));
            assert!(errors.values().expect("codec observations").is_empty());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
}

/// A fresh retention observation cannot hide the next withdrawal of either demand bit.
#[test]
fn withdrawal_after_retirement_renewal_is_observed() {
    for global in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").expect("endpoint");
        let address = listener.local_addr().expect("endpoint address");
        let demand = Rc::<DemandState>::default();
        let plan = Plan::<KeyTag<0>, 1>::new(
            1,
            Spec::new(Duration::from_secs(2), Duration::from_secs(2)).expect("retry bounds"),
        )
        .expect("one connection");
        let config = plan.configured().expect("watched connector resources");
        let mut reports = Reports::<_>::new(config, [Id::fixed::<0>()]);
        let mut finish = Completion::new();
        Runtime::new(config).with_scope(
            |mut runtime| {
                let target = Session::<codec::Outbound>::default();
                let events = target.events.clone();
                let audit = target.audit.clone();
                let errors = target.errors.clone();
                let opened = target.gate.clone();
                let setup = Setup::new(
                    target,
                    dope_manifold::client::session::ingress::Plan::none(),
                );
                let set = Snapshot::<_, 1>::from_one(Revision::new(1), Endpoint::new(0, address));
                let hash = runtime.hash_state(Domain::DEFAULT);
                let installed = plan
                    .bind(Local::new(Identity), hash)
                    .watch::<_, _, _, Replace, _, Bundle<Tcp, Identity, Balanced>, 1, _, _>(
                        setup,
                        set,
                        Demand(demand.clone()),
                        &mut runtime,
                        Scope::driver,
                        |host, runtime| {
                            let now = runtime.driver().driver_ref().clock().now();
                            runtime
                                .with_at_for(
                                    &mut finish,
                                    Host::new(Limited { inner: host }),
                                    now.into(),
                                    Duration::from_secs(2),
                                    |mut app| {
                                        app.drive_for(
                                            now.into(),
                                            Duration::from_secs(2),
                                            Until(|| opened.hits() == 1),
                                        )
                                        .expect("connected endpoint")
                                        .into_output();
                                        demand.pending.set(None);
                                        app.drive(now.into(), Until(|| true))
                                            .expect("settled demand")
                                            .into_output();
                                        let step = Cell::new(0);
                                        app.drive_for(
                                            now.into(),
                                            Duration::from_secs(2),
                                            Until(|| {
                                                if let Some(&retained) =
                                                    [false, true, false].get(step.get())
                                                {
                                                    assert!(
                                                        Events::snapshot(&events).closed.is_empty(),
                                                        "renewal must preserve the first epoch"
                                                    );
                                                    demand.needed.set(!global || retained);
                                                    demand
                                                        .live
                                                        .set((global || retained).then_some(0));
                                                    step.set(step.get() + 1);
                                                }
                                                Events::snapshot(&events).closed.iter().any(
                                                    |(_, reason)| {
                                                        *reason == Reason::EndpointRetired
                                                    },
                                                )
                                            }),
                                        )
                                        .expect("second withdrawal remains observable")
                                        .into_output();
                                        assert_eq!(step.get(), 3);
                                        assert_eq!(opened.hits(), 1);
                                    },
                                )
                                .expect("application teardown");
                        },
                    );
                assert!(installed.is_ok(), "install watched connector");
                audit.checked((0, 0));
                assert!(errors.values().expect("codec observations").is_empty());
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
    }
}

/// Renewing an active epoch also resumes an already ready dial.
#[test]
fn retirement_renewal_resumes_ready_dial() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("endpoint");
    let address = listener.local_addr().expect("endpoint address");
    let chosen = Rc::new(Trace::new());
    let limit = Rc::new(Cell::new(NonZero::<usize>::MIN));
    let demand = Rc::<DemandState>::default();
    let plan = Plan::<KeyTag<0>, 2>::new(
        2,
        Spec::new(Duration::from_secs(2), Duration::from_secs(2)).expect("retry bounds"),
    )
    .expect("two connections");
    let config = plan.configured().expect("watched connector resources");
    let mut reports = Reports::<_>::new(config, [Id::fixed::<0>()]);
    let mut finish = Completion::new();
    Runtime::new(config).with_scope(
        |mut runtime| {
            let target = Session::<codec::Outbound>::default();
            let events = target.events.clone();
            let audit = target.audit.clone();
            let errors = target.errors.clone();
            let opened = target.gate.clone();
            let setup = Setup::new(
                target,
                dope_manifold::client::session::ingress::Plan::none(),
            );
            let set = Snapshot::<_, 1>::from_one(
                Revision::new(1),
                Endpoint::new(
                    0,
                    Address {
                        addr: address,
                        chosen: chosen.clone(),
                    },
                ),
            );
            let hash = runtime.hash_state(Domain::DEFAULT);
            let installed = plan
                .bind(Local::new(Identity), hash)
                .watch::<_, _, _, Replace, _, Bundle<Net, Identity, Balanced>, 1, _, _>(
                    setup,
                    set,
                    LimitedDemand {
                        inner: Demand(demand.clone()),
                        chosen: chosen.clone(),
                        limit: limit.clone(),
                    },
                    &mut runtime,
                    Scope::driver,
                    |host, runtime| {
                        let now = runtime.driver().driver_ref().clock().now();
                        runtime
                            .with_at_for(
                                &mut finish,
                                Host::new(Limited { inner: host }),
                                now.into(),
                                Duration::from_secs(2),
                                |mut app| {
                                    app.drive_for(
                                        now.into(),
                                        Duration::from_secs(2),
                                        Until(|| opened.hits() == 1),
                                    )
                                    .expect("one active and one ready dial")
                                    .into_output();
                                    let step = Cell::new(0);
                                    assert!(matches!(
                                        app.drive_for(
                                            now.into(),
                                            Duration::from_millis(100),
                                            Renewal {
                                                demand: &demand,
                                                limit: &limit,
                                                step: &step,
                                            },
                                        ),
                                        Err(drive::Error::Timeout)
                                    ));
                                    assert_eq!(step.get(), 2);
                                    assert_eq!(opened.hits(), 2, "renewal resumes the ready dial");
                                    assert_eq!(
                                        &*chosen.values().expect("selected endpoints"),
                                        &[address, address]
                                    );
                                    assert!(Events::snapshot(&events).closed.is_empty());
                                },
                            )
                            .expect("application teardown");
                    },
                );
            assert!(installed.is_ok(), "install watched connector");
            audit.checked((0, 0));
            assert!(errors.values().expect("codec observations").is_empty());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
}

/// Replacement and withdrawal remove pending retries before the same slot is reused.
#[test]
fn retired_retries() {
    let mut finish = Completion::new();
    let refused = TcpListener::bind("127.0.0.1:0").expect("reserve refused address");
    let bad = refused.local_addr().expect("refused address");
    let listener = peer::TcpAcceptor::bind((Ipv4Addr::LOCALHOST, 0).into()).expect("endpoint");
    drop(refused);
    let good = listener.local_addr().expect("endpoint address");
    let pending = Rc::new(Cell::new(Some(
        Snapshot::<Endpoint<u32, SocketAddr>, 1>::from_one(
            Revision::new(1),
            Endpoint::new(10, bad),
        ),
    )));
    let failures = Gate::new();
    let changes = Rc::new(Trace::new());
    let plan = Plan::<KeyTag<0>, 1>::new(
        1,
        Spec::new(Duration::from_secs(2), Duration::from_secs(4)).expect("valid retry bounds"),
    )
    .expect("single connection");
    let config = plan.configured().expect("discovery resources");
    let mut reclaim2 = Reports::<_>::new(config, [Id::fixed::<0>()]);

    Runtime::new(config).with_scope(
        |mut runtime| {
            use dope_manifold::client::session::ingress::Plan;

            let target = Session::<codec::Outbound>::default();
            let events = target.events.clone();
            let errors = target.errors.clone();
            let audit = target.audit.clone();
            let opened = target.gate.clone();
            let hash = runtime.hash_state(Domain::DEFAULT);
            let result = plan
                .bind(Local::new(Identity), hash)
                .discover::<_, _, _, Preserve, _, _, Bundle<Tcp, Identity, Balanced>, 1, _, _>(
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
                        let result = runtime.with_at_for(
                            &mut finish,
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
                                pending.set(Some(
                                    Snapshot::<Endpoint<u32, SocketAddr>, 1>::from_one(
                                        Revision::new(2),
                                        Endpoint::new(11, bad),
                                    ),
                                ));
                                app.drive(at(2000), Until(|| failures.hits() == 3))
                                    .expect("new endpoint starts its own backoff")
                                    .into_output();
                                let earlier =
                                    timer.earliest().expect("replacement backoff is armed");
                                assert!(at(3000) <= earlier && earlier < at(4000));
                                pending.set(Some(
                                    Snapshot::<Endpoint<u32, SocketAddr>, 1>::from_one(
                                        Revision::new(3),
                                        Endpoint::new(20, good),
                                    ),
                                ));
                                app.drive(at(2001), Until(|| opened.hits() == 1))
                                    .expect("replacement connects before old retry")
                                    .into_output();
                                drop(
                                    listener
                                        .accept_for(GUARD)
                                        .expect("accept replacement")
                                        .expect("connected endpoint"),
                                );
                                app.drive(at(2002), Until(|| failures.hits() == 4))
                                    .expect("replacement enters backoff")
                                    .into_output();
                                pending.set(Some(Snapshot::<Endpoint<u32, SocketAddr>, 1>::empty(
                                    Revision::new(4),
                                )));
                                app.drive(
                                    at(2003),
                                    Until(|| {
                                        changes.values().expect("discovery observations").len() == 8
                                    }),
                                )
                                .expect("withdraw backed-off endpoint")
                                .into_output();
                                app.drive(at(5000), Until(|| true))
                                    .expect("removed retry deadlines are quiescent")
                                    .into_output();
                                assert_eq!(failures.hits(), 4);
                                assert_eq!(opened.hits(), 1);
                                pending.set(Some(
                                    Snapshot::<Endpoint<u32, SocketAddr>, 1>::from_one(
                                        Revision::new(5),
                                        Endpoint::new(30, good),
                                    ),
                                ));
                                app.drive(at(5001), Until(|| opened.hits() == 2))
                                    .expect("withdrawn slot accepts a fresh endpoint")
                                    .into_output();
                                let second = listener
                                    .accept_for(GUARD)
                                    .expect("accept fresh endpoint")
                                    .expect("connected endpoint");
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
                                app.drive(
                                    at(5002),
                                    Until(|| {
                                        changes.values().expect("discovery observations").len()
                                            == 12
                                    }),
                                )
                                .expect("withdraw connected endpoint")
                                .into_output();
                                drop(second);
                                app.drive(
                                    at(5003),
                                    Until(|| {
                                        let observed = events.take();
                                        let closed = observed.closed.len();
                                        events.set(observed);
                                        closed == 2
                                    }),
                                )
                                .expect("retired epoch closes without health feedback")
                                .into_output();
                                pending.set(Some(Snapshot::<Endpoint<u32, SocketAddr>, 1>::empty(
                                    Revision::new(7),
                                )));
                                app.drive(
                                    at(5004),
                                    Until(|| {
                                        changes.values().expect("discovery observations").len()
                                            == 14
                                    }),
                                )
                                .expect("observe discovery after retired failure")
                                .into_output();
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
            assert!(
                observed.is_ordered(),
                "{:?}",
                (&observed.conn, &observed.closed, &observed.quarantined)
            );
        },
        recovery::Policy::new(|event| reclaim2.push(event), recover),
    );
    assert_reclaimed(&reclaim2);

    let changes = changes.values().expect("discovery observations");
    assert_eq!(changes.len(), 14);
    for (index, id) in [Some(10), Some(11), Some(20), None, Some(30), None, None]
        .into_iter()
        .enumerate()
    {
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
}
