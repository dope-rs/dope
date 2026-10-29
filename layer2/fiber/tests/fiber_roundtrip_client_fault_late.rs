use dope::{core::driver::life::recovery, net::wire::domain::Local, runtime::executor::session};
use dope_test::harness::{peer::AcceptScript, scenario};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{convert::Infallible, io, io::Read, rc::Rc, time::Duration};

use dope::{
    core::{
        driver::route::{Id, table::conn::Capacity},
        platform::wake,
    },
    manifold::timing::Balanced,
    net::tcp::Tcp,
    runtime::executor::drive,
};
use dope_fiber::{BlockOn as _, net::client::Plan};
use dope_test::{fibers, reclaim::Reports};

use crate::common::network::rounds::client::fault::late::*;

#[test]
fn reuses_after_late_error() {
    let mut finish = session::Completion::new();
    let state = Rc::new(State::new());
    let (rx, tx) = wake::Ends::blocking().expect("reuse release").split();
    let (ack, accepted) = wake::Ends::blocking()
        .expect("reuse acknowledgement")
        .split();
    AcceptScript::with(
        move |peer| -> io::Result<()> {
            let mut first = peer.accept()?;
            first.read_wait(Some(Duration::from_secs(5)))?;
            let mut body = Vec::new();
            first.read_to_end(&mut body)?;

            let next = peer.accept();
            assert!(next.is_ok(), "second connection must succeed");
            rx.wait().expect("second connection may close after reuse");
            accepted
                .notify()
                .expect("acknowledge second accept and reuse");
            Ok(())
        },
        |server| {
            let addr = server.addr();

            let plan = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
            let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
            let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

            runtime.with_input(
                plan,
                |mut sess| {
                    let plan = sess.storage();
                    let store =
                        (plan.try_build(sess.driver().storage)).expect("client port storage");
                    let result = store.machine_in(
                        Local::new(Rc::clone(&state)),
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
                                    {
                                        let opened = app.block_on(conn.connect(addr))?;
                                        assert!(opened.is_ok(), "first connection must succeed");
                                    }
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &state.retired, 1)
                                            .expect("bounded progress"),
                                        fibers::Wait::Reached
                                    );

                                    let opened = app.block_on(conn.connect(addr))?;
                                    assert!(opened.is_ok(), "reused connection must succeed");
                                    assert!(
                                        state.is_done(),
                                        "the late EOF failure was not processed before lane reuse"
                                    );
                                    tx.notify()
                                        .expect("server reuse gate must remain connected");
                                    ack.wait()
                                        .expect("server acknowledged second accept and reuse");
                                    Ok(())
                                },
                            ))
                            .expect("late-error test application must finish");
                            ((driven).expect("late-error fiber stalled"));
                        },
                    );
                    assert!(result.is_ok(), "late-error connector must be constructed");
                    server
                        .finish()
                        .expect("late-error server thread must finish")
                        .expect("late-error server operations must succeed");
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
    .expect("late-error server must start");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
