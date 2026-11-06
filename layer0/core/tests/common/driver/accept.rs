use std::{
    net::SocketAddr,
    ops::ControlFlow,
    time::{Duration, Instant},
};

use dope_core::{
    driver::{life, result::poll, retained::accept::Readiness},
    io::event::{Kind, net::accept::Completion},
};
use dope_test::harness::peer::{Peer, Stream};

use super::Wait;

pub fn connect<'d>(
    scope: &mut life::Scope<'_, 'd>,
    address: SocketAddr,
) -> (Stream<'static>, Completion<'d>) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !matches!(Readiness::from(scope.driver_ref()), Readiness::Ready) {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "descriptor retirement deadline");
        {
            let (_, mut driver, mut schedule) = scope.split();
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
        };
    }
    let peer = Peer::at(address).try_connect().expect("peer");
    let mut completed = None;
    while completed.is_none() {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "accept deadline");
        {
            let (_, mut driver, mut schedule) = scope.split();
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
        };
    }
    (peer, completed.expect("native accept completion"))
}
