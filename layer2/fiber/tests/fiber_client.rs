use std::net::SocketAddr;

use dope::{
    core::driver::life::recovery,
    net::{link::event::ConnectFault, wire::domain::Local},
    runtime::executor::session::Completion,
};
use dope_fiber::net::client::ConnectError;
use dope_test::harness::scenario;
pub mod common;
use common::{
    recover,
    runtime::{self, Setup},
};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::{app::Host, timing::Balanced},
    net::{link::event, tcp::Tcp, wire::Identity},
};
use dope_fiber::{BlockOn as _, net::client};
use dope_test::reclaim::Reports;

#[test]
fn failed_dial_reuses_slot() {
    let mut finish = Completion::new();
    let setup = Setup::for_profile::<Balanced>();
    let mut reports = Reports::<_>::new(setup.config(), [Id::fixed::<0>()]);

    setup.with_input(
        client::Plan::<Tcp, Identity>::new(Capacity::fixed::<1>(), Identity),
        |mut session| {
            let plan = session.storage();
            let storage = (plan.try_build(session.driver().storage)).expect("client port storage");
            let handle = storage.handle();
            let built = storage.machine_in(
                Local::new(Identity),
                &mut session,
                |session| session.driver(),
                |conn, session| {
                    session.with_quiesce(
                        &mut finish,
                        Host::new(conn),
                        scenario::TIMEOUT,
                        |mut app| {
                            for _ in 0..2 {
                                let result = app
                                    .block_on(handle.connect(SocketAddr::from(([127, 0, 0, 1], 0))))
                                    .expect("connect fiber stalled");
                                assert!(
                                    matches!(
                                        result,
                                        Err(ConnectError::Transport(ConnectFault::Transport(
                                            event::ConnectError::Connect(_)
                                        )))
                                    ),
                                    "unsupported target must report the failed connect operation"
                                );
                            }
                        },
                    )
                },
            );
            assert!(
                matches!(built, Ok(Ok(()))),
                "client machine or teardown failed"
            );
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
