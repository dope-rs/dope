use std::convert::Infallible;

use dope::{
    core::driver::{
        life::{
            creation::Attempt,
            recovery::{Policy, Recovery},
        },
        settings,
    },
    manifold::timing::Throughput,
    runtime::executor::{Executor, session::Scope},
};
use dope_dns::config;
use dope_test::scope::Scoped;
pub mod common;

use dope::core::driver::life::domain;
use dope_dns::storage::{Error, Plan};
use o3::depot::index::bitmap;

use crate::common::{Extent, fixture};

#[test]
fn rejects_foreign_extent() {
    let mut create = Attempt::new();
    let servers = config::Servers::try_new([fixture::SERVER]).expect("fixture servers");
    let source = Plan::new(fixture::configure(servers, 1)).expect("source plan");
    let servers = config::Servers::try_new([fixture::SERVER]).expect("fixture servers");
    let target = Plan::new(fixture::configure(servers, 1)).expect("target plan");
    let executor = Executor::new(&mut create, settings::Config::for_profile::<Throughput>());
    let executor = executor.ok().expect("executor construction");
    executor
        .with_plan(Extent { source, target })
        .run(|executor| {
            let result = executor
                .try_enter(
                    Scoped::new(|_: Scope<'_, '_, '_, '_, Extent>| ()),
                    Policy::new(
                        |report| fixture::shutdown(report).expect("DNS shutdown"),
                        |failure: Recovery<'_, '_>| -> Infallible {
                            panic!("DNS recovery: {:?}", failure.cause())
                        },
                    ),
                )
                .map_err(|error| error.map_storage(|rejected| rejected.map_op(Scoped::into_inner)));
            assert!(matches!(
                result,
                Err(domain::Error::Storage(domain::Rejected {
                    cause: Error::Entries(bitmap::Error {
                        index: 2,
                        capacity: 2
                    }),
                    ..
                }))
            ));
        });
}
