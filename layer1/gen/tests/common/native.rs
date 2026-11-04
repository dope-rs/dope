use std::convert::Infallible;

use engine::{
    core::{
        driver::{
            life::{creation::Attempt, recovery::Recovery},
            settings,
        },
        io::Reclaimed,
    },
    manifold::timing::Throughput,
    runtime::executor::{Executor, create},
};
pub fn try_executor(attempt: &mut Attempt) -> Result<Executor, create::Error<'_>> {
    Executor::new(attempt, settings::Config::for_profile::<Throughput>())
}

pub fn shutdown(event: Reclaimed) {
    assert!(
        matches!(event, Reclaimed::Shutdown),
        "unexpected operation: {event:?}"
    );
}

pub fn recover(failure: Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause());
}
