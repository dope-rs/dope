use dope::{
    core::driver::life::recovery, net::wire::domain::Local, runtime::executor::session::Completion,
};
use dope_test::harness::{peer::AcceptScript, scenario};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{
    convert::Infallible,
    io,
    io::{Read, Write},
    pin::Pin,
    rc::Rc,
    time::Duration,
};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::tcp::Tcp,
};
use dope_fiber::{BlockOn as _, abi::Task, net::client::Plan};
use dope_test::{fibers, reclaim::Reports};

use crate::common::network::rounds::client::{
    Ping,
    gated::{App, Probe, Wire},
};

#[test]
fn reuses_settled_slot() {
    let mut finish = Completion::new();
    const RESPONSE: &[u8] = b"PONG-after-settled-cancel";

    AcceptScript::with(
        |peer| -> io::Result<()> {
            let mut old = peer.accept()?;
            old.read_wait(Some(Duration::from_secs(2)))?;
            old.write_all(b"H")?;
            let mut byte = [0; 1];
            assert_eq!(
                old.read(&mut byte)?,
                0,
                "settled cancellation must actively close the unclaimed socket"
            );

            let mut new = peer.accept()?;
            new.write_all(b"H")?;
            let mut req = [0; 4];
            new.read_exact(&mut req)?;
            assert_eq!(&req, b"ping");
            new.write_all(RESPONSE)
        },
        |server| {
            let addr = server.addr();

            let probe = Rc::new(Probe::default());
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
                        Local::new(Rc::clone(&probe)),
                        &mut sess,
                        |sess| sess.driver(),
                        |port, sess| {
                            let handle = store.handle();
                            let app = App { connector: port };

                            let driven = (sess.with_quiesce(
                                &mut finish,
                                app,
                                scenario::TIMEOUT,
                                |mut app| {
                                    {
                                        let mut old = Task::new(handle.connect(addr));
                                        let first = app
                                            .block_on(fibers::TEST.poll_once(Pin::new(&mut old)))?;
                                        assert!(
                                            first.is_none(),
                                            "connect must own a pending ticket"
                                        );

                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &probe.established, 1)
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

                                    let opened = app.block_on(handle.connect(addr))?;
                                    let next = (opened).ok().expect(
                                        "the replacement connection must open after retirement",
                                    );
                                    Ping::try_ping(&mut app, next).map(|result| {
                                        assert_eq!(
                                            result.ok().expect("complete ping roundtrip"),
                                            RESPONSE
                                        )
                                    })
                                },
                            ))
                            .expect("the settled-cancel application must tear down cleanly");
                            ((driven).expect("settled cancel fiber stalled"));
                        },
                    );
                    assert!(
                        result.is_ok(),
                        "the settled-cancel connector must build from its storage"
                    );
                    server
                        .finish()
                        .expect("the settled-cancel reply-server thread must join")
                        .expect("the settled-cancel reply server must complete both accepts");
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
        },
    )
    .expect("the settled-cancel reply-server thread must start");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
