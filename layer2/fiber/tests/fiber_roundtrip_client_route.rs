use std::convert::Infallible;

use dope::{
    core::{
        driver::{
            life::recovery,
            route::{App as _, Id},
        },
        io::fd::outbound,
    },
    manifold::app::creation::Error,
    runtime::executor::session::Completion,
};
use dope_test::harness::{peer::AcceptScript, scenario};
use o3::depot::capacity::Count;

use crate::common::runtime::{self, Setup};
pub mod common;

use dope::{
    core::driver::{
        route::table::conn::Capacity,
        settings::{claim::Needs, errors::Claim},
    },
    manifold::timing::Balanced,
    net::{tcp::Tcp, wire::Identity},
};
use dope_core::driver::route::KeyTag;
use dope_fiber::{BlockOn as _, net::client::Plan};
use dope_test::reclaim::Reports;

use crate::common::network::rounds::client::{App, MAX_CONN, Ping, try_reply};

#[test]
fn owns_outbound_slots() {
    const CONNECTIONS: usize = 1025;

    let port = Plan::<Tcp, Identity>::new(Capacity::fixed::<CONNECTIONS>(), Identity);
    let config = (Needs::from_tree(&port).and_then(|needs| needs.resolve::<Balanced>()))
        .expect("outbound slot configuration must resolve");

    assert!((config.file_slots().accept().count()) == (Count::new(0)));
    assert_eq!(config.file_slots().outbound().count().get(), CONNECTIONS);
}

#[test]
fn rejects_duplicate_route() {
    let first = Plan::<Tcp, Identity>::new(Capacity::fixed::<1>(), Identity);
    let second = Plan::<Tcp, Identity>::new(Capacity::fixed::<1>(), Identity);
    let refs = (&first, &second);
    let result = Needs::from_tree(&refs).and_then(|needs| needs.resolve::<Balanced>());
    assert!(matches!(result,
        Err(Claim::DuplicateRoute(id)) if id == KeyTag::<0>::ID
    ));
}

#[test]
fn roundtrip() {
    let mut finish = Completion::new();
    let reply: &[u8] = b"PONG-fiber-connector-roundtrip";
    AcceptScript::with(
        |peer| try_reply(peer, reply, 1),
        |server| {
            let addr = server.addr();

            let plan = Plan::<Tcp, Identity>::new(Capacity::fixed::<MAX_CONN>(), Identity);
            let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
            let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

            runtime.with_input(
                plan,
                |mut sess| {
                    let plan = sess.storage();
                    let store =
                        (plan.try_build(sess.driver().storage)).expect("client port storage");
                    let result = store.machine(
                        &mut sess,
                        |sess| sess.driver(),
                        |port, sess| {
                            let second = store.machine(sess, |sess| sess.driver(), |_, _| ());
                            assert!(matches!(
                                second,
                                Err(Error::Descriptor(outbound::Error::Occupied))
                            ));
                            let conn = store.handle();
                            let app = App { connector: port };
                            let driven = (sess.with_quiesce(
                                &mut finish,
                                app,
                                scenario::TIMEOUT,
                                |mut app| {
                                    let opened =
                                        app.block_on(dope_fiber::fiber!('_ => async move {
                                            conn.connect(addr).await
                                        }))?;
                                    let io = (opened)
                                        .ok()
                                        .expect("the roundtrip connector must open its connection");
                                    Ping::try_ping(&mut app, io).map(|result| {
                                        assert_eq!(
                                            result.ok().expect("complete ping roundtrip"),
                                            reply
                                        )
                                    })
                                },
                            ))
                            .expect("the connector roundtrip application must tear down cleanly");
                            ((driven).expect("roundtrip fiber stalled"));
                        },
                    );
                    assert!(result.is_ok(), "the connector route must bind once");
                    server
                        .finish()
                        .expect("the roundtrip reply-server thread must join")
                        .expect("the roundtrip reply server must complete its accept");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                self::finish,
            );
            assert!(
                reports
                    .events()
                    .expect("reclamation observations")
                    .is_empty(),
                "unexpected reclaimed operations: {reports:?}"
            );
        },
    )
    .expect("reply server");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
