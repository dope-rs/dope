pub mod common;

use std::{convert::Infallible, io::Write as _, rc::Rc, time::Duration};

use common::{
    manifold::listener::trace::{self, App, Connect as _, One},
    scenario::{RuntimeCase as _, case::listener::Listener},
};
use dope_core::{
    driver::{
        life::{creation::Attempt, recovery},
        route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
        settings::{Resource as _, claim::Needs},
    },
    io::{
        Reclaimed,
        event::{batch::Flow, net::accept},
        socket::Backlog,
    },
};
use dope_manifold::{
    app::{
        env::{Bundle, quota::PerIp},
        receive::Borrowed,
    },
    network::listener::{self, topology::Layout},
    timing::Throughput,
};
use dope_net::{
    tcp::Tcp,
    wire::{Identity, domain},
};
use dope_runtime::executor::{
    Executor,
    session::{self, Completion},
};
use dope_test::{
    fibers::Wait,
    harness::{
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

#[test]
fn local_slots_reuse() {
    for (per_ip, burst) in [
        (PerIp::UNLIMITED, false),
        (PerIp::fixed::<1>(), false),
        (PerIp::UNLIMITED, true),
    ] {
        let mut finish = Completion::new();
        let first = Rc::new(trace::Trace::default());
        let second = Rc::new(trace::Trace::default());
        let accepted = [Gate::new(), Gate::new()];
        let left = Layout::<KeyTag<0>, Borrowed>::new(Capacity::fixed::<1>());
        let right = Layout::<KeyTag<1>, Borrowed>::new(Capacity::fixed::<1>());
        let needs = left.include_in(Needs::new()).expect("first route");
        let settings = right
            .include_in(needs)
            .expect("second route")
            .resolve::<Throughput>()
            .expect("listener resources");
        let mut creation = Attempt::new();
        let executor = Executor::new(&mut creation, settings);
        executor
            .as_ref()
            .map_err(|error| error.kind())
            .expect("executor");
        let executor = executor.ok().expect("executor");
        let mut reports = Reports::<_>::new(settings, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        executor.run(|executor| {
            let result = executor.enter_with(
                |mut session| {
                    let mut config = One::config();
                    config.per_ip = Some(per_ip);
                    if burst {
                        config.backlog = Backlog::fixed::<4>();
                    }
                    let hash = session.hash_state(listener::Domain::for_listener(0));
                    listener::Install::<KeyTag<0>, Bundle<Tcp, Identity, Throughput>, _>::new(
                        config,
                        domain::Local::new(Identity),
                    ).prepare(
                        hash,
                        &mut session,
                        session::Scope::driver,
                        |prepared, session| {
                            let left = prepared.build(
                                App { trace: first.clone(), gate: accepted[0].clone(), observe: trace::check },
                                &mut session.driver(),
                            ).expect("bind prepared first listener");
                            let a = left.local_addr();
                            let hash = session.hash_state(listener::Domain::for_listener(1));
                            let mut config = One::config();
                            config.per_ip = Some(per_ip);
                            listener::Install::<KeyTag<1>, Bundle<Tcp, Identity, Throughput>, _>::new(
                                config,
                                domain::Local::new(Identity),
                            ).with_driver(
                                App { trace: second.clone(), gate: accepted[1].clone(), observe: trace::check },
                                hash,
                                session,
                                session::Scope::driver,
                                |right, session| {
                                    let b = right.local_addr();
                                    session.with_quiesce(
                                        &mut finish,
                                        trace::Pair { first: left, second: right },
                                        Duration::from_secs(2),
                                        |mut host| {
                                            let held = Peer::at(a).try_connect().expect("first peer");
                                            let extra = burst.then(|| a.connect_n(2).expect("burst peers"));
                                            let waiting = burst.then(|| Peer::at(b).try_connect().expect("queued second peer"));
                                            assert_eq!(host.try_wait(&accepted[0], 1).expect("first accept"), Wait::Reached);
                                            let peer = waiting.unwrap_or_else(|| Peer::at(b).try_connect().expect("second peer"));
                                            if let Some(extra) = extra {
                                                for peer in extra {
                                                    Peer::try_reset(peer).expect("surplus reset");
                                                }
                                                Peer::try_reset(peer).expect("second reset");
                                                drop(held);
                                                assert_eq!(host.try_wait(&first.closed, 1).expect("first close"), Wait::Reached);
                                                let want = accepted[1].hits();
                                                if want != 0 {
                                                    assert_eq!(host.try_wait(&second.closed, want).expect("second close"), Wait::Reached);
                                                }
                                                let mut peer = Peer::at(a).try_connect().expect("first recovery");
                                                peer.write_all(b"first").expect("first data");
                                                assert_eq!(host.try_wait(&first.received, 1).expect("first receive"), Wait::Reached);
                                                let mut peer = Peer::at(b).try_connect().expect("second recovery");
                                                peer.write_all(b"second").expect("second data");
                                                assert_eq!(host.try_wait(&second.received, 1).expect("second receive"), Wait::Reached);
                                                return;
                                            }
                                            assert_eq!(host.try_wait(&accepted[1], 1).expect("second accept"), Wait::Reached);
                                            assert_eq!(first.closed.hits(), 0);
                                            drop(peer);
                                            assert_eq!(host.try_wait(&second.closed, 1).expect("second close"), Wait::Reached);
                                            let peer = Peer::at(b).try_connect().expect("reused peer");
                                            assert_eq!(host.try_wait(&accepted[1], 2).expect("reuse accept"), Wait::Reached);
                                            assert_eq!(first.closed.hits(), 0);
                                            drop(peer);
                                            assert_eq!(host.try_wait(&second.closed, 2).expect("reuse close"), Wait::Reached);
                                            drop(held);
                                        },
                                    ).expect("both listeners retire");
                                },
                            ).expect("second listener");
                        },
                    ).expect("first listener");
                },
                recovery::Policy::new(|event| reports.push(event), |error: recovery::Recovery<'_, '_>| -> Infallible { panic!("recovery: {:?}", error.cause()) }),
            );
            result.as_ref().map_err(|error| error.cause()).expect("listener runtime");
        });
        if burst {
            assert!(reports.events().expect("reclamation").iter().all(|event| {
                matches!(event, Reclaimed::AcceptRoute {
                    cause: accept::Failure::Syscall(error), flow: Flow::Last, ..
                } if error.code().get() == 23)
            }));
        } else {
            assert!(reports.events().expect("reclamation").is_empty());
            assert_eq!((first.accepted.get(), second.accepted.get()), (1, 2));
            assert_eq!((first.closed.hits(), second.closed.hits()), (1, 2));
        }
    }
}

#[test]
fn burst_recovers() {
    let mut finish = Completion::new();
    let trace = Rc::new(trace::Trace::default());
    let gate = Gate::new();
    let prepared = Listener::new::<1>(Default::default())
        .per_ip(PerIp::UNLIMITED)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            App {
                trace: trace.clone(),
                gate: gate.clone(),
                observe: trace::check,
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let held = Peer::at(case.addr()).try_connect().expect("first peer");
                let extra = Peer::at(case.addr()).try_connect().expect("burst peer");
                assert_eq!(
                    case.try_wait(&gate, 1).expect("first accept"),
                    Wait::Reached
                );
                Peer::try_reset(extra).expect("surplus reset");
                drop(held);
                assert_eq!(
                    case.try_wait(&trace.closed, 1).expect("first close"),
                    Wait::Reached
                );
                let mut peer = Peer::at(case.addr()).try_connect().expect("recovery peer");
                peer.write_all(b"recovered").expect("recovery data");
                assert_eq!(
                    case.try_wait(&trace.received, 1).expect("recovery receive"),
                    Wait::Reached
                );
            },
            recovery::Policy::new(
                |event| reports.push(event),
                |error: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("recovery: {:?}", error.cause())
                },
            ),
            |outcome| outcome.expect("native lifecycle"),
        )
        .expect("listener run");
    assert!(reports.events().expect("reclamation").is_empty());
}
