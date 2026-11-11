use std::convert::Infallible;

use engine::{
    core::{
        driver::{
            life::{
                creation::Attempt,
                recovery::{Policy, Recovery},
            },
            settings,
        },
        io::Reclaimed,
    },
    manifold::timing::Throughput,
    runtime::executor::{Executor, session},
};

pub fn run<R>(
    run: impl for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd>) -> R,
) -> R {
    let mut attempt = Attempt::new();
    let executor = match Executor::new(&mut attempt, settings::Config::for_profile::<Throughput>())
    {
        Ok(executor) => executor,
        Err(error) => panic!("runtime creation: {:?}", error.kind()),
    };
    executor.run(
        |executor| match executor.enter_with(run, Policy::new(shutdown, recover)) {
            Ok(output) => output,
            Err(error) => panic!("runtime completion: {:?}", error.cause()),
        },
    )
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
