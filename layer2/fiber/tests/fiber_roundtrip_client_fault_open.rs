use dope::{
    core::driver::life::recovery, manifold::client::attempt::queue::mailbox::Error,
    net::link::pool::change::opening::Failure, runtime::executor::session::Completion,
};
use dope_test::harness::scenario;

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{convert::Infallible, net::SocketAddr};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::tcp::Tcp,
    runtime::executor::drive,
};
use dope_fiber::{
    BlockOn as _,
    abi::Task,
    net::client::{ConnectError, Plan},
};
use dope_test::reclaim::Reports;
use o3::depot::arena::slab::Vacancy;

use crate::common::network::rounds::client::fault::open::*;

#[test]
fn returns_owned_error() {
    let mut finish = Completion::new();
    let addr = SocketAddr::from(([127, 0, 0, 1], 9));

    let plan = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
    let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

    runtime.with_input(
        plan,
        |mut sess| {
            let plan = sess.storage();
            let store = plan
                .try_build(sess.driver().storage)
                .expect("client port storage");
            let built = store.machine(
                &mut sess,
                |sess| sess.driver(),
                |port, sess| {
                    let handle = store.handle();

                    let app = App { connector: port };
                    (((sess.with_quiesce(&mut finish, app, scenario::TIMEOUT, |mut app| {
                        let outcome = app.block_on(handle.connect(addr))?;
                        let error = outcome.err().expect("wire open must fail");
                        assert!(matches!(
                            error,
                            ConnectError::Open(Failure::Wire(error))
                                if error.cause().0 == "permanent fiber wire open failure"
                        ));
                        Ok::<(), drive::Error>(())
                    }))
                    .expect("the owned-open-error application must tear down cleanly"))
                    .expect("owned-open-error fiber stalled"));
                },
            );
            assert!(
                built.is_ok(),
                "the open-failure connector must build from its storage"
            );
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
}

#[test]
fn keeps_unread_error() {
    let mut finish = Completion::new();
    let addr = SocketAddr::from(([127, 0, 0, 1], 9));

    let plan = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
    let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

    runtime.with_input(
        plan,
        |mut sess| {
            let plan = sess.storage();
            let store = plan
                .try_build(sess.driver().storage)
                .expect("client port storage");
            let built = store.machine(
                &mut sess,
                |sess| sess.driver(),
                |port, sess| {
                    let handle = store.handle();
                    let app = App { connector: port };

                    let exchange =
                        (sess.with_quiesce(&mut finish, app, scenario::TIMEOUT, |mut app| {
                            app.block_on(Reuse {
                                first: Task::new(handle.connect(addr)),
                                second: Task::new(handle.connect(addr)),
                                phase: Phase::Start,
                            })
                        }))
                        .ok()
                        .expect("the unread-error application must tear down cleanly");
                    let (first, second) = exchange
                        .expect("unread-error fiber stalled")
                        .ok()
                        .expect("exact open outcomes");

                    let first = match first {
                        ConnectError::Open(Failure::Wire(first)) => first,
                        other => {
                            assert!(
                                matches!(other, ConnectError::Open(Failure::Wire(_))),
                                "the original typed open failure must survive until its first poll"
                            );
                            return;
                        }
                    };
                    assert_eq!(first.cause().0, "permanent fiber wire open failure");
                    assert!(matches!(
                        second,
                        ConnectError::Mailbox(Error::Queue(Vacancy::Full))
                    ));
                },
            );
            assert!(
                built.is_ok(),
                "the open-failure connector must build from its storage"
            );
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
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
