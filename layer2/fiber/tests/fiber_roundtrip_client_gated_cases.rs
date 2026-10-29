use dope::{
    core::driver::{life::recovery, settings::Receive, storage::Value},
    net::wire::domain::Local,
    runtime::executor::session::Completion,
};
use dope_test::harness::scenario;

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{convert::Infallible, rc::Rc};

use dope::{
    core::driver::{
        route::{Id, table::conn::Capacity},
        settings::claim::Needs,
    },
    manifold::timing::Balanced,
    net::tcp::Tcp,
    runtime::executor::{Executor, drive},
};
use dope_fiber::{BlockOn as _, net::client::Plan};
use dope_test::{fibers, harness::signals::Progress as _, reclaim::Reports};
use o3::buffer::Source as _;

use crate::common::network::rounds::client::{
    Collect, MAX_CONN, Ping,
    gated::{App, Probe, Wire},
};

#[test]
fn parks_until_receive() {
    let mut finish = Completion::new();
    let reply: &[u8] = b"PONG-gated";
    let request = Probe::try_reply(reply, |addr, probe| {
        let plan = Plan::<Tcp, Wire>::new(Capacity::fixed::<MAX_CONN>(), ());
        let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
        let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

        runtime.with_input(
            plan,
            |mut sess| {
                let plan = sess.storage();
                let store = (plan.try_build(sess.driver().storage)).expect("client port storage");
                let result = store.machine_in(
                    Local::new(Rc::clone(&probe)),
                    &mut sess,
                    |sess| sess.driver(),
                    |link, sess| {
                        let conn = store.handle();

                        let app = App { connector: link };
                        let driven = (sess.with_quiesce(
                            &mut finish,
                            app,
                            scenario::TIMEOUT,
                            |mut app| -> Result<(), drive::Error> {
                                let opened = app.block_on(conn.connect(addr))?;
                                let io = (opened).ok().expect("gated connection must succeed");
                                Ping::try_ping(&mut app, io).map(|result| {
                                    assert_eq!(result.ok().expect("complete ping roundtrip"), reply)
                                })
                            },
                        ))
                        .expect("gated test application must finish");
                        ((driven).expect("gated roundtrip fiber stalled"));
                    },
                );
                assert!(result.is_ok(), "gated connector must be constructed");
                assert!(
                    probe.attempts.get() <= 4,
                    "wire send path spun {} times before receive",
                    probe.attempts.get(),
                );
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            self::finish,
        );
        assert!(
            reports
                .events()
                .expect("reclamation observations")
                .is_empty()
                && !matches!(probe.release.get(), Some(Err(_))),
            "unexpected reclaimed operations: {reports:?}"
        );
    })
    .expect("gated peers")
    .expect("gated peer I/O");
    assert_eq!(request, *b"ping");
}

#[test]
fn credit_resumes_receive() {
    let mut finish = Completion::new();
    static RESPONSE: [u8; 32 * 1024] = [b'R'; 32 * 1024];
    let request = Probe::try_reply(&RESPONSE, |addr, probe| {
        let port = Plan::<Tcp, Wire>::new(Capacity::fixed::<MAX_CONN>(), ());
        let cfg = ((Needs::from_tree(&port).and_then(|needs| needs.resolve::<Balanced>()))
            .expect("credited driver configuration must resolve"))
        .with_receive(Receive::fixed::<64, 1024>());
        use dope::core::driver::life::creation::Attempt;
        let mut create = Attempt::new();
        let result = Executor::new(&mut create, cfg);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("credited executor must be constructed");
        let Ok(exec) = result else {
            return;
        };
        let exec = exec.with_plan(Value::new(port));
        let mut reports = Reports::<_>::new(cfg, [Id::fixed::<0>()]);

        exec.run(|executor| {
            let result = executor.enter_with(
                |mut sess| {
                    let plan = sess.storage();
                    let store =
                        (plan.try_build(sess.driver().storage)).expect("client port storage");
                    let result = store.machine_in(
                        Local::new(Rc::clone(&probe)),
                        &mut sess,
                        |sess| sess.driver(),
                        |link, sess| {
                            let conn = store.handle();

                            let app = App { connector: link };
                            let driven = (sess.with_quiesce(
                                &mut finish,
                                app,
                                scenario::TIMEOUT,
                                |mut app| -> Result<(), drive::Error> {
                                    let opened = app.block_on(conn.connect(addr))?;
                                    let io =
                                        (opened).ok().expect("credited connection must succeed");
                                    let mut io = io;
                                    let sent = app.block_on(io.write_all(b"ping"))?;
                                    assert!(sent.is_ok(), "credited request must be written");
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &probe.retained, 1)
                                            .expect("bounded progress"),
                                        fibers::Wait::Reached
                                    );
                                    let mut got = Vec::with_capacity(RESPONSE.len());
                                    while got.len() < RESPONSE.len() {
                                        let read = app.block_on(io.read())?;
                                        let lease = (read)
                                            .ok()
                                            .expect("credited response must remain open")
                                            .expect("credited response must remain open");
                                        let bytes = Collect::try_into_vec(lease)
                                            .expect("consume received bytes");
                                        assert!(
                                            !bytes.is_empty(),
                                            "connection closed before deferred data drained"
                                        );
                                        got.extend_from_slice(&bytes);
                                    }
                                    assert_eq!(got, RESPONSE);
                                    Ok(())
                                },
                            ))
                            .expect("credited test application must finish");
                            ((driven).expect("credited fiber stalled"));

                            assert!(
                                probe.released.get() > 0,
                                "dropping a retained cursor must return its receive credit"
                            );
                        },
                    );
                    assert!(result.is_ok(), "credited connector must be constructed");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            assert!(
                reports
                    .events()
                    .expect("reclamation observations")
                    .is_empty()
                    && !matches!(probe.release.get(), Some(Err(_))),
                "unexpected reclaimed operations: {reports:?}"
            );

            assert!(result.is_ok(), "credited scenario must finish");
        });
    })
    .expect("gated peers")
    .expect("gated peer I/O");
    assert_eq!(request, *b"ping");
}

#[test]
fn drop_returns_credit() {
    let mut finish = Completion::new();
    static RESPONSE: [u8; 1024] = [b'Q'; 1024];
    let request = Probe::try_reply(&RESPONSE, |addr, probe| {
        let port = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
        let cfg = ((Needs::from_tree(&port).and_then(|needs| needs.resolve::<Balanced>()))
            .expect("drop-credit driver configuration must resolve"))
        .with_receive(Receive::fixed::<8, 1024>());
        use dope::core::driver::life::creation::Attempt;
        let mut create = Attempt::new();
        let result = Executor::new(&mut create, cfg);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("drop-credit executor must be constructed");
        let Ok(exec) = result else {
            return;
        };
        let exec = exec.with_plan(Value::new(port));
        let mut reports = Reports::<_>::new(cfg, [Id::fixed::<0>()]);

        exec.run(|executor| {
            let result = executor.enter_with(
                |mut sess| {
                    let plan = sess.storage();
                    let store =
                        (plan.try_build(sess.driver().storage)).expect("client port storage");
                    let result = store.machine_in(
                        Local::new(Rc::clone(&probe)),
                        &mut sess,
                        |sess| sess.driver(),
                        |link, sess| {
                            let conn = store.handle();
                            let app = App { connector: link };

                            let driven = (sess.with_quiesce(
                                &mut finish,
                                app,
                                scenario::TIMEOUT,
                                |mut app| -> Result<(), drive::Error> {
                                    {
                                        let opened = app.block_on(conn.connect(addr))?;
                                        let mut io = (opened)
                                            .ok()
                                            .expect("drop-credit connection must succeed");
                                        let sent = app.block_on(io.write_all(b"ping"))?;
                                        assert!(
                                            sent.is_ok(),
                                            "drop-credit request must be written"
                                        );
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &probe.retained, 1)
                                                .expect("bounded progress"),
                                            fibers::Wait::Reached
                                        );
                                    }
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &probe.retired, 1)
                                            .expect("bounded progress"),
                                        fibers::Wait::Reached
                                    );
                                    assert!(
                                        probe.released.get() > 0,
                                        "detaching Io must drop its unread credited cursor"
                                    );
                                    Ok(())
                                },
                            ))
                            .expect("drop-credit test application must finish");
                            ((driven).expect("drop-credit fiber stalled"));
                        },
                    );
                    assert!(result.is_ok(), "drop-credit connector must be constructed");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            assert!(
                reports
                    .events()
                    .expect("reclamation observations")
                    .is_empty()
                    && !matches!(probe.release.get(), Some(Err(_))),
                "unexpected reclaimed operations: {reports:?}"
            );

            assert!(result.is_ok(), "drop-credit scenario must finish");
        });
    })
    .expect("gated peers")
    .expect("gated peer I/O");
    assert_eq!(request, *b"ping");
}

#[test]
fn credit_resumes_close() {
    let mut finish = Completion::new();
    static RESPONSE: [u8; 1024] = [b'W'; 1024];
    let request = Probe::try_reply(&RESPONSE, |addr, probe| {
        let port = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
        let cfg = ((Needs::from_tree(&port).and_then(|needs| needs.resolve::<Balanced>()))
            .expect("close-credit driver configuration must resolve"))
        .with_receive(Receive::fixed::<8, 1024>());
        use dope::core::driver::life::creation::Attempt;
        let mut create = Attempt::new();
        let exec = (Executor::new(&mut create, cfg))
            .ok()
            .expect("close-credit executor must be constructed");
        let exec = exec.with_plan(Value::new(port));
        let mut reports = Reports::<_>::new(cfg, [Id::fixed::<0>()]);

        exec.run(|executor| {
    let result = executor.enter_with(
            |mut sess| {
                let plan = sess.storage();
                let store = (plan.try_build(sess.driver().storage)).expect("client port storage");
                let result = store.machine_in(
                    Local::new(Rc::clone(&probe)),
                    &mut sess,
                    |sess| sess.driver(),
                    |link, sess| {
                        let conn = store.handle();
                        let app = App { connector: link };

                        let driven =
                        (sess.with_quiesce(&mut finish, app, scenario::TIMEOUT, |mut app| -> Result<(), drive::Error> {
                            {
                                let opened = app.block_on(conn.connect(addr))?;
                                let mut io =
                                    (opened).ok().expect("close-credit connection must succeed");
                                let sent = app.block_on(io.write_all(b"ping"))?;
                                assert!(sent.is_ok(), "close-credit request must be written");
                                {
                                    let read = app.block_on(io.read())?;
                                    let lease = (read)
                                        .ok()
                                        .expect("close-credit read must retain a lease")
                                        .expect("close-credit read must retain a lease");

                                    assert_eq!(lease.remaining(), RESPONSE.len());
                                    assert_eq!(
                                        probe.retired.hits(),
                                        0,
                                        "the live lease must retain its exact connection generation"
                                    );
                                }
                                assert_eq!(fibers::TEST.try_wait(&mut app, &probe.eof, 1).expect("bounded progress"), fibers::Wait::Reached);
                                assert_eq!(fibers::TEST.try_wait(&mut app, &probe.retired, 1).expect("bounded progress"), fibers::Wait::Reached);
                            }
                            assert!(probe.released.get() > 0);
                            Ok(())
                        }))
                        .expect("close-credit test application must finish");
                        ((driven).expect("close-credit fiber stalled"));
                    },
                );
                assert!(result.is_ok(), "close-credit connector must be constructed");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.events().expect("reclamation observations").is_empty() && !matches!(probe.release.get(),Some(Err(_))),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "close-credit scenario must finish");
        });
}).expect("gated peers")
    .expect("gated peer I/O");
    assert_eq!(request, *b"ping");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
