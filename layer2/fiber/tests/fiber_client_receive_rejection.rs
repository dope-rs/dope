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
    harness::{peer::TcpAcceptor, scenario, signals::Progress as _},
    reclaim::Reports,
};
use o3::depot::queue::tagged::entry;

pub mod common;
use common::recover;

use common::{
    network::rounds::client::fault::park::{State, Wire},
    runtime::Setup,
};

#[test]
fn keeps_receive_rejection() {
    let listener = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("listener");
    let addr = listener.local_addr().expect("listener address");
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
                    let peer = scope
                        .with_quiesce(
                            &mut finish,
                            Host::new(machine),
                            scenario::TIMEOUT,
                            |mut app| {
                                let mut io = app
                                    .block_on(port.handle().connect(addr))?
                                    .ok()
                                    .expect("connect");
                                let peer = listener
                                    .accept_for(scenario::TIMEOUT)
                                    .expect("accept")
                                    .expect("connected peer");
                                peer.set_nodelay(true).expect("peer nodelay");
                                peer.read_wait(Some(scenario::TIMEOUT))
                                    .expect("peer read timeout");
                                peer.write_wait(Some(scenario::TIMEOUT))
                                    .expect("peer write timeout");
                                peer.write_all(b"p").expect("prefix");
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &state.received.handle(), 1)
                                        .expect("receive prefix"),
                                    fibers::Wait::Reached
                                );
                                peer.write_all(b"r").expect("registration input");
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
                                Ok::<_, drive::Error>(peer)
                            },
                        )
                        .expect("quiescence")
                        .expect("fiber completion");
                    assert_eq!(peer.read(&mut [0]).expect("closed peer"), 0);
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
}
