use std::convert::Infallible;

pub mod common;

use common::{
    process::{Cpus as _, Restart},
    scenario::{app::Empty, rt::Configured},
};
use dope_core::{
    driver::life::{creation::Attempt, recovery},
    platform::{affinity::Cpu, wake},
};
use dope_runtime::{executor::session, process::Layout};

fn main() {
    for enabled in [false, true] {
        let cpus = <[Cpu; 1]>::try_current().expect("one available worker CPU");
        let (wait, ready) = wake::Ends::blocking().expect("worker readiness").split();
        let runtime = Layout::pinned(cpus)
            .and_then(|layout| layout.try_assign([ready]))
            .expect("one process worker");
        let mut storage = std::mem::MaybeUninit::uninit();
        let result = runtime
            .run_watched(
                o3::mem::slot::Vacant::new(&mut storage),
                |ready, context| {
                    let mut create = Attempt::new();
                    let created = Configured::quic::<2, 8>().try_source(&mut create, context);
                    created
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("worker executor");
                    let executor = created.ok().expect("worker executor");
                    let mut reported = None;
                    let request = executor.run(|executor| {
                        let entered = executor.enter_with(
                            |mut session| {
                                let mut finish = session::Completion::new();
                                for _ in 0..3 {
                                    session
                                        .with_app(&mut finish, Empty {}, |_| ())
                                        .expect("repeated installation");
                                }
                                ready.notify().expect("worker readiness");
                                let request = session.with_app(&mut finish, Empty {}, |app| {
                                    app.run().expect("worker shutdown")
                                });
                                request
                                    .as_ref()
                                    .map_err(|error| error.cause())
                                    .expect("worker session");
                                request.ok().expect("worker session")
                            },
                            recovery::Policy::new(
                                |event| {
                                    reported.get_or_insert(event);
                                },
                                recover,
                            ),
                        );
                        entered
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("worker teardown");
                        entered.ok().expect("worker teardown")
                    });
                    assert!(reported.is_none(), "native reclamation: {reported:?}");
                    Ok::<_, Infallible>(request)
                },
                Restart {
                    ready: wait,
                    heartbeat: enabled,
                },
            )
            .map_err(|error| {
                error.fold(Vec::new(), |mut faults, fault| {
                    faults.push(fault);
                    faults
                })
            });
        assert!(
            result.is_ok(),
            "process installation, heartbeat {enabled}: {result:?}"
        );
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
