use dope::{core::driver::life::recovery, net::wire::domain::Local, runtime::executor::session};
use dope_test::harness::{
    GUARD,
    peer::{self, TcpAcceptor},
    scenario,
};

use crate::common::runtime::{self, Setup};
pub mod common;
use common::recover;

use std::{net::Ipv4Addr, rc::Rc, task::Poll};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::{app::Host, timing::Balanced},
    net::tcp::Tcp,
    runtime::executor::drive,
};
use dope_fiber::{BlockOn as _, abi::poll_fn, net::client::Plan};
use dope_test::{fibers, reclaim::Reports};

use crate::common::network::rounds::client::fault::late::*;

#[test]
fn reuses_after_late_error() {
    let mut finish = session::Completion::new();
    let state = Rc::new(State::new());
    let listener = TcpAcceptor::bind((Ipv4Addr::LOCALHOST, 0).into()).expect("bind");
    let addr = listener.local_addr().expect("address");
    let plan = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
    let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

    runtime.with_input(
        plan,
        |mut sess| {
            let plan = sess.storage();
            let store = (plan.try_build(sess.driver().storage)).expect("client port storage");
            let result = store.machine_in(
                Local::new(Rc::clone(&state)),
                &mut sess,
                |sess| sess.driver(),
                |port, sess| {
                    let conn = store.handle();
                    let app = Host::new(port);

                    let driven = (sess.with_quiesce(
                        &mut finish,
                        app,
                        scenario::TIMEOUT,
                        |mut app| -> Result<(), drive::Error> {
                            let io = app
                                .block_on(conn.connect(addr))?
                                .ok()
                                .expect("first connection must succeed");
                            let first = listener
                                .accept_for(GUARD)
                                .expect("first accept")
                                .expect("first connection");
                            drop(io);
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &state.retired, 1)
                                    .expect("bounded progress"),
                                fibers::Wait::Reached
                            );
                            first.nonblock(true);
                            assert_eq!(
                                fibers::TEST
                                    .try_run(
                                        &mut app,
                                        poll_fn(|context| match first.read(&mut [0]) {
                                            Err(peer::Error::Syscall(error))
                                                if error.is_block() || error.is_intr() =>
                                            {
                                                context.wake();
                                                Poll::Pending
                                            }
                                            result => Poll::Ready(result),
                                        }),
                                    )
                                    .expect("first close progress")
                                    .expect("first EOF"),
                                0
                            );

                            let opened = app.block_on(conn.connect(addr))?;
                            assert!(opened.is_ok(), "reused connection must succeed");
                            let _next = listener
                                .accept_for(GUARD)
                                .expect("second accept")
                                .expect("reused connection");
                            assert!(
                                state.is_done(),
                                "the late close failure was not processed before lane reuse"
                            );
                            Ok(())
                        },
                    ))
                    .expect("late-error test application must finish");
                    ((driven).expect("late-error fiber stalled"));
                },
            );
            assert!(result.is_ok(), "late-error connector must be constructed");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(
        reports
            .events()
            .expect("reclamation observations")
            .is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
