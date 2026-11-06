pub mod common;

use std::{
    io::{self, Read as _},
    net::TcpStream,
    time::Duration,
};

use common::{
    manifold::listener::{
        finish::{seal, teardown::try_config},
        trace::One,
    },
    recover,
};
use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
};
use dope_manifold::{
    app::env::{Bundle, quota::PerIp},
    network::listener,
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::Identity, wire::domain};
use dope_runtime::{
    executor::{self, drain, session, session::Completion},
    shutdown,
};
use dope_test::{harness::peer::Peer, reclaim::Reports};
use o3::base::time::Positive;

#[test]
fn retires_listener_while_existing_connection_drains() {
    for per_ip in [None, Some(PerIp::fixed::<4>())] {
        let mut config = One::config();
        config.connections = Capacity::fixed::<4>();
        config.per_ip = per_ip;
        let settings = try_config(config.connections).expect("listener settings");
        let mut reports = Reports::new(settings, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        let (source, trigger) = shutdown::Pair::new().expect("shutdown pair").split();
        let mut attempt = Attempt::new();
        let executor = executor::Build::try_build(source, &mut attempt, settings);
        executor
            .as_ref()
            .map_err(|error| error.kind())
            .expect("create executor");
        executor.ok().expect("executor").run(|executor| {
            executor
                .enter_with(
                    |mut session| {
                        let hash = session.hash_state(listener::Domain::DEFAULT);
                        listener::Install::<KeyTag<0>, Bundle<Tcp, Identity, Throughput>, _>::new(
                            config,
                            domain::Local::new(Identity),
                        )
                        .with_driver(
                            seal::App::default(),
                            hash,
                            &mut session,
                            session::Scope::driver,
                            |listener, _wait, session| {
                                let addr = listener.local_addr();
                                let mut finish = Completion::new();
                                Peer::at(addr)
                                    .with(
                                        move |peer| {
                                                        let mut byte = [0];
                                                        peer.write_all(b"a").expect("before seal");
                                                        peer.read_exact(&mut byte).expect("reply");
                                                        assert_eq!(byte, *b"a");
                                                        trigger.fire().expect("begin drain");
                                                        peer.read_exact(&mut byte)
                                                            .expect("seal notice");
                                                        assert_eq!(byte, *b"s");
                                                        match TcpStream::connect_timeout(
                                                            &addr,
                                                            Duration::from_secs(2),
                                                        ) {
                                                            Ok(mut fresh) => {
                                                                fresh
                                                                    .set_read_timeout(Some(
                                                                        Duration::from_secs(2),
                                                                    ))
                                                                    .expect("fresh read timeout");
                                                                match fresh.read(&mut byte) {
                                                                    Ok(0) => {}
                                                                    Err(error)
                                                                        if matches!(
                                                                            error.kind(),
                                                                            io::ErrorKind::ConnectionReset
                                                                                | io::ErrorKind::ConnectionAborted
                                                                        ) => {}
                                                                    other => panic!(
                                                                        "sealed backlog must close: {other:?}"
                                                                    ),
                                                                }
                                                            }
                                                            Err(error) => assert_eq!(
                                                                error.kind(),
                                                                io::ErrorKind::ConnectionRefused,
                                                                "sealed listener connect"
                                                            ),
                                                        }
                                                        peer.write_all(b"b").expect("after seal");
                                                        peer.read_exact(&mut byte)
                                                            .expect("retained connection reply");
                                                        assert_eq!(byte, *b"b");
                                                        assert_eq!(peer.read(&mut byte).expect("EOF"), 0);
                                        },
                                        |peer| {
                                            session
                                                .with_quiesce(
                                                    &mut finish,
                                                    seal::Host { listener },
                                                    Duration::from_secs(2),
                                                    |app| {
                                                        let report = app
                                                            .run_drain(Positive::from_secs::<5>())
                                                            .expect("cooperative drain");
                                                        assert!(
                                                            report.outcome() == drain::Outcome::Quiesced,
                                                            "existing connection drains cooperatively"
                                                        );
                                                    },
                                                )
                                                .expect("finish listener");
                                            // Native finish follows cooperative drain before the
                                            // peer waiting for the descriptor's EOF is joined.
                                            peer.finish().expect("peer result");
                                        },
                                    )
                                    .expect("peer script");
                            },
                        )
                        .expect("install listener");
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                )
                .as_ref()
                .map_err(|error| error.cause())
                .expect("finish executor");
        });
        assert!(reports.events().expect("reclaimed events").is_empty());
    }
}
