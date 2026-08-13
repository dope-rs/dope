use std::{io, time::Duration};

use dope_core::driver::settings;
use dope_manifold::{
    service::health::{Backoff, Domain},
    timing::Balanced,
};
use dope_runtime::executor::Executor;

fn error(base: Duration) -> Option<io::ErrorKind> {
    let config = settings::Config::for_tcp_profile::<Balanced>(1).expect("driver config");
    Executor::new(config)
        .expect("executor")
        .enter(|mut session| {
            let state = session.hash_state(Domain::DEFAULT);
            Backoff::new(base, state).err().map(|error| error.kind())
        })
}

#[test]
fn rejects_zero_backoff() {
    assert_eq!(error(Duration::ZERO), Some(io::ErrorKind::InvalidInput));
}

#[test]
fn rejects_backoff_outside_the_duration_range() {
    assert_eq!(error(Duration::MAX), Some(io::ErrorKind::InvalidInput));
}
