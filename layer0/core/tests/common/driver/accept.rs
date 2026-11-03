use std::{
    net::{SocketAddr, TcpStream},
    ops::ControlFlow,
    time::{Duration, Instant},
};

use dope_core::{
    driver::{life, result::poll, retained::accept::Readiness},
    io::event::{Kind, net::accept::Completion},
};

use super::Wait;

pub fn connect<'d>(
    scope: &mut life::Scope<'_, 'd>,
    address: SocketAddr,
) -> (TcpStream, Completion<'d>) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !matches!(Readiness::from(scope.driver_ref()), Readiness::Ready) {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "descriptor retirement deadline");
        scope.with_driver(|mut driver, mut schedule| {
            let mut turn = schedule.begin();
            Wait {
                driver: &mut driver,
                turn: &mut turn,
                timeout: left,
            }
            .try_run()
            .as_ref()
            .map_err(super::Failure::cause)
            .expect("descriptor retirement");
        });
    }
    let peer = TcpStream::connect(address).expect("peer");
    let mut completed = None;
    while completed.is_none() {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "accept deadline");
        scope.with_driver(|mut driver, mut schedule| {
            let mut turn = schedule.begin();
            driver.wait(turn.reactor(), Some(left)).expect("accept");
            let dispatched = driver.dispatch(turn.reactor(), |event, _| {
                let done = match event.into_kind() {
                    Kind::Accept(done) => Some(done),
                    _ => None,
                }
                .expect("accept event");
                assert!(completed.replace(done).is_none(), "one accept");
                ControlFlow::Continue(())
            });
            assert!(matches!(
                dispatched,
                poll::Dispatch::Ready { retained: None, .. }
            ));
        });
    }
    (peer, completed.expect("native accept completion"))
}
