use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_test::harness::{peer::TcpScript, scenario, signals::Gate};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{convert::Infallible, io::Write, num::NonZero};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::{tcp::Tcp, wire::domain},
    runtime::executor::drive,
};
use dope_fiber::{
    BlockOn as _,
    net::{
        client::Plan,
        error::read::{Edge, Local, Read},
        read::Config,
    },
};
use dope_test::{fibers, reclaim::Reports};
use o3::buffer::Source as _;

use crate::common::network::rounds::client::fault::{
    Error,
    capacity::*,
    read::{App, Context, Wire},
};

#[test]
fn retains_both_failures() {
    let mut finish = Completion::new();
    let seen = Gate::new();
    TcpScript::with(
        |stream| {
            let data = vec![b'p'; TERMINAL_AFTER_BUFFER_CAP];
            stream
                .write_all(&data)
                .expect("capacity payload must be written");
        },
        |server| {
            let addr = server.addr();

            let plan = Plan::<Tcp, Wire<State>>::new(Capacity::fixed::<1>(), ())
                .with_receive(Config {
                    slots: NonZero::new(256).expect("receive slots"),
                    chunks: NonZero::new(256).expect("receive chunks"),
                    bytes: NonZero::new(BUFFER_CAP as u32).expect("receive bytes"),
                })
                .expect("receive plan");
            let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
            let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

            runtime.with_input(
                plan,
                |mut sess| {
                    let plan = sess.storage();
                    let store =
                        (plan.try_build(sess.driver().storage)).expect("client port storage");
                    let result = store.machine_in(
                        domain::Local::new(Context::<State> {
                            cause: "domain-borrowed terminal failure",
                            binding: seen.clone(),
                        }),
                        &mut sess,
                        |sess| sess.driver(),
                        |port, sess| {
                            let conn = store.handle();
                            let app = App { connector: port };
                            let driven = (sess.with_quiesce(
                                &mut finish,
                                app,
                                scenario::TIMEOUT,
                                |mut app| -> Result<(), drive::Error> {
                                    let opened = app.block_on(conn.connect(addr))?;
                                    let mut io =
                                        (opened).ok().expect("capacity connection must succeed");
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &seen, 1)
                                            .expect("bounded progress"),
                                        fibers::Wait::Reached
                                    );
                                    let mut bytes = 0;
                                    let error =
                                        loop {
                                            match app.block_on(io.read())? {
                                                Ok(lease) => bytes += lease
                                                    .expect(
                                                        "wire failure must not collapse into close",
                                                    )
                                                    .remaining(),
                                                Err(error) => break error,
                                            }
                                        };
                                    let closed = app.block_on(io.read())?;
                                    assert!(bytes <= BUFFER_CAP);
                                    assert!(matches!(error,
                                        Read::LocalThenEdge {
                                            local: Local::BufferedBytesTooLarge {
                                                actual,
                                                maximum,
                                            },
                                            edge: Edge::Wire(error),
                                        } if actual.get() == TERMINAL_AFTER_BUFFER_CAP as u64
                                            && maximum.get() == BUFFER_CAP as u64
                                            && matches!(error.causes(), (Error::Protocol(
                                                "domain-borrowed terminal failure"
                                            ), None))
                                    ));
                                    assert!(
                                        matches!(closed, Ok(None)),
                                        "both failure causes must be emitted together once"
                                    );
                                    Ok(())
                                },
                            ))
                            .expect("capacity test application must finish");
                            ((driven).expect("capacity fiber stalled"));
                        },
                    );
                    assert!(result.is_ok(), "capacity connector must be constructed");

                    server.finish().expect("capacity server must finish");
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
    .expect("capacity server must start");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
