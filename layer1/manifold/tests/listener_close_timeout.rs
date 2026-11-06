pub mod common;

use common::manifold::listener::finish::close::{script, wire::Wire};
use common::{recover, scenario::Runtime};
use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{app::env::Bundle, network::listener::conn::deadline::Kind};
use dope_net::tcp::Tcp;
use dope_runtime::executor::session::{self, Completion};
use dope_test::{
    harness::{
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

#[test]
fn wait_keeps_deadlines() {
    use std::time::Duration;

    use common::manifold::listener::defer::{script::Until, wire::Profile};
    use dope_core::{
        driver::{
            route::table::conn::Capacity,
            settings::{Resource as _, claim::Needs},
        },
        io::socket,
    };
    use dope_manifold::{
        app::{Host, receive},
        network::listener::{self, config, handler, topology},
    };
    use dope_net::{link::egress, wire::domain};
    use o3::base::time::Positive;

    for draining in [false, true] {
        let mut finish = Completion::new();
        let entered = Gate::new();
        let release = Gate::new();
        let received = Gate::new();
        let closed = Gate::new();
        let layout = topology::Layout::<KeyTag<0>, receive::Borrowed>::new(Capacity::fixed::<1>());
        let settings = layout
            .include_in(Needs::new())
            .expect("listener resources")
            .resolve::<Profile>()
            .expect("listener settings");
        let mut reports = Reports::<_>::new(settings, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

        Runtime::new(settings).with_scope(
            |mut session| {
                let config = config::Prepared {
                    connections: Capacity::fixed::<1>(),
                    per_ip: None,
                    direct_flights: 1.into(),
                    bind: ([127, 0, 0, 1], 0).into(),
                    backlog: socket::Backlog::fixed::<1>(),
                    stream: Default::default(),
                    transport: Default::default(),
                    egress: egress::Config::shared(egress::Credit::fixed::<0, 0>()),
                };
                let hash = session.hash_state(listener::Domain::DEFAULT);
                let wire = Wire::waiting(entered.clone(), release.clone());
                let install = listener::Install::<KeyTag<0>, Bundle<Tcp, Wire, Profile>, _>::new(
                    config,
                    domain::Local::new(wire),
                );
                let install = if draining {
                    install.with_life(handler::Lifetime::graceful(
                        Positive::from_millis::<20>(),
                        Positive::from_millis::<20>(),
                    ))
                } else {
                    install
                };
                let result = install.with_driver(
                    script::App {
                        payload: (!draining).then(|| b"R".to_vec()),
                        close: closed.clone(),
                        chunk: Some(received.clone()),
                        timeout: Some(if draining { Kind::Drain } else { Kind::Send }),
                    },
                    hash,
                    &mut session,
                    session::Scope::driver,
                    |server, _wait, session| {
                        let addr = server.local_addr();
                        let start = session.driver().driver_ref().clock().now().into();
                        let bounded = session
                            .with_at_for(
                                &mut finish,
                                Host::new(server),
                                start,
                                Duration::from_secs(2),
                                |mut app| {
                                    let peer = Peer::at(addr).try_connect().expect("connect peer");
                                    peer.write_all(b"REQ").expect("write request");
                                    app.drive_for(
                                        start,
                                        Duration::from_secs(1),
                                        Until {
                                            gate: &received,
                                            hits: 1,
                                        },
                                    )
                                    .expect("receive request")
                                    .into_output();
                                    let begin = start
                                        .try_add(Duration::from_millis(if draining {
                                            21
                                        } else {
                                            0
                                        }))
                                        .expect("close coordinate");
                                    app.drive_for(
                                        begin,
                                        Duration::from_secs(1),
                                        Until {
                                            gate: &entered,
                                            hits: 1,
                                        },
                                    )
                                    .expect("close waits for wire storage")
                                    .into_output();
                                    assert_eq!(closed.hits(), 0);
                                    let due = start
                                        .try_add(Duration::from_millis(if draining {
                                            42
                                        } else {
                                            101
                                        }))
                                        .expect("deadline coordinate");
                                    let bounded = app
                                        .drive_for(
                                            due,
                                            Duration::from_millis(200),
                                            Until {
                                                gate: &closed,
                                                hits: 1,
                                            },
                                        )
                                        .is_ok();
                                    assert_eq!(release.hits(), 0);
                                    release.hit();
                                    bounded
                                },
                            )
                            .expect("listener teardown");
                        assert!(bounded, "close wait lost its deadline; draining={draining}");
                    },
                );
                assert!(result.is_ok(), "listener installation");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(reports.events().expect("reclaimed events").is_empty());
    }
}

#[test]
fn shutdown_keeps_deadline() {
    use common::{manifold::listener::defer::wire::Profile, scenario::case::listener::Listener};
    use dope_test::fibers::Wait;

    let mut finish = Completion::new();
    let entered = Gate::new();
    let release = Gate::new();
    let received = Gate::new();
    let closed = Gate::new();
    let prepared = Listener::new::<1>(Default::default())
        .direct(1)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Profile>>((
            script::App {
                payload: None,
                close: closed.clone(),
                chunk: Some(received.clone()),
                timeout: Some(Kind::Send),
            },
            Wire::waiting(entered.clone(), release.clone()),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let result = prepared.with_run(
        &mut finish,
        |case| {
            let peer = Peer::at(case.addr()).try_connect().expect("connect peer");
            peer.write_all(b"REQ").expect("write request");
            assert_eq!(
                case.try_wait(&received, 1).expect("receive request"),
                Wait::Reached,
            );
            peer
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |result| result.expect("listener lifecycle"),
    );
    assert_ne!(entered.hits(), 0, "shutdown reaches the held wire");
    assert_eq!(release.hits(), 0);
    let _peer = result.expect("shutdown keeps send deadline");
    assert_eq!(closed.hits(), 1);
}
