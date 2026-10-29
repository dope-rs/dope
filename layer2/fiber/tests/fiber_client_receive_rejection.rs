use std::{
    convert::Infallible,
    io::{Read as _, Write as _},
};

use dope::{
    core::driver::{
        life::recovery,
        route::{Id, table::conn::Capacity},
    },
    manifold::{app::Host, timing::Balanced},
    net::{link::event::ParkError, tcp::Tcp, wire::domain},
    runtime::executor::{drive, session::Completion},
};
use dope_fiber::{
    BlockOn as _,
    net::{client::Plan, error::read::Read},
};
use dope_test::{
    fibers,
    harness::{peer::TcpScript, scenario, signals::Progress as _},
    reclaim::Reports,
};
use o3::depot::queue::tagged::entry;

pub mod common;

use common::{
    network::rounds::client::fault::park::{State, Wire},
    runtime::Setup,
};

#[test]
fn keeps_receive_rejection() {
    TcpScript::with(
        |peer| {
            peer.write_all(b"p").expect("prefix");
            peer.read_exact(&mut [0]).expect("prefix admitted");
            peer.write_all(b"r").expect("registration input");
            assert_eq!(peer.read(&mut [0]).expect("closed peer"), 0);
        },
        |peer| {
            let addr = peer.addr();
            let state = State::default();
            let plan = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
            let setup = Setup::try_for_plan::<Balanced, _>(&plan).expect("driver configuration");
            let mut reports = Reports::new(setup.config(), [Id::fixed::<0>()]);
            setup.with_input(
                plan,
                |mut scope| {
                    let mut finish = Completion::new();
                    let port = scope
                        .storage()
                        .try_build(scope.driver().storage)
                        .expect("port");
                    let result = port.machine_in(
                        domain::Local::new(&state),
                        &mut scope,
                        |scope| scope.driver(),
                        |machine, scope| {
                            scope
                                .with_quiesce(
                                    &mut finish,
                                    Host::new(machine),
                                    scenario::TIMEOUT,
                                    |mut app| -> Result<(), drive::Error> {
                                        let mut io = app
                                            .block_on(port.handle().connect(addr))?
                                            .ok()
                                            .expect("connect");
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &state.received.handle(), 1)
                                                .expect("receive prefix"),
                                            fibers::Wait::Reached
                                        );
                                        assert!(app.block_on(io.write_all(b"a"))?.is_ok());
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &state.rejected.handle(), 1)
                                                .expect("register rejection"),
                                            fibers::Wait::Reached
                                        );
                                        let prefix = app
                                            .block_on(io.read())?
                                            .ok()
                                            .flatten()
                                            .expect("buffered prefix");
                                        assert_eq!(prefix.as_ref(), b"p");
                                        drop(prefix);
                                        assert!(matches!(
                                            app.block_on(io.read())?,
                                            Err(Read::Receive(ParkError::Register(
                                                entry::Error::Index { index: 1, capacity }
                                            ))) if capacity == Capacity::fixed::<1>()
                                        ));
                                        assert!(matches!(app.block_on(io.read())?, Ok(None)));
                                        Ok(())
                                    },
                                )
                                .expect("quiescence")
                                .expect("fiber completion");
                        },
                    );
                    assert!(result.is_ok(), "machine construction");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                |result| {
                    result.expect("driver lifecycle");
                },
            );
            assert!(reports.events().expect("native reports").is_empty());
            assert_eq!(
                state.released.handle().hits(),
                0,
                "no committed reservation"
            );
            assert!(state.cancelled.get().is_none_or(|slot| slot == 0));
            peer.finish().expect("peer completion");
        },
    )
    .expect("peer startup");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
