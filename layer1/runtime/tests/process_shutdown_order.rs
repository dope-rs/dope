use std::convert::Infallible;

pub mod common;

use common::{
    process::{Cpus as _, Ordered, Stop},
    scenario::{app::Empty, rt::Configured},
};
use dope_core::{
    dope_sys::io::socket::record::Record,
    driver::life::{creation::Attempt, recovery},
    platform::{affinity::Cpu, channel::Channel},
};
use dope_runtime::{executor::session, process::Layout};

fn main() {
    let cpus = <[Cpu; 2]>::try_current().expect("two available worker CPUs");
    let (wait, exit) = Channel::pair().expect("worker retirement");
    let runtime = Layout::pinned(cpus)
        .and_then(|layout| layout.try_assign([Ordered::First(exit), Ordered::Last(wait)]))
        .expect("two process workers");
    let mut storage = std::mem::MaybeUninit::uninit();
    let result = runtime
        .watch_gated(
            o3::mem::slot::Vacant::new(&mut storage),
            |ordered, context, gate| {
                let wait = ordered.install().expect("worker exit observation");
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
                            let request = session.with_app(&mut finish, Empty {}, |app| {
                                let drain = gate.open().expect("worker gate");
                                let request = app.run().expect("worker shutdown");
                                if let Some(wait) = wait {
                                    let observed = wait.recv();
                                    assert!(
                                        matches!(observed, Record::Closed),
                                        "first worker retired: {observed:?}"
                                    );
                                }
                                drain.finish().expect("worker drain");
                                request
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
            Stop,
        )
        .map_err(|error| {
            error.fold(Vec::new(), |mut faults, fault| {
                faults.push(fault);
                faults
            })
        });
    assert!(result.is_ok(), "ordered process shutdown: {result:?}");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
