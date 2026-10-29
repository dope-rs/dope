//! Explicit macro paths resolve the real renamed Cargo dependencies.

use std::convert::Infallible;

use engine::core::driver::life::recovery::Recovery;
pub mod common;

use common::{macros::Text, renames::*};
use engine::{
    core::{
        driver::{life::recovery::Policy, route, storage},
        io::Reclaimed,
    },
    manifold::timing,
    runtime::{executor::session::Completion, topology},
};

#[test]
fn compile_alone() {
    let text = String::from("renamed");
    let plan = storage::With::new(Text(&text), 7);
    let domain = topology::Domain::for_profile::<timing::Throughput>(plan)
        .expect("resolve renamed storage layout");
    let mut report = Completion::new();
    domain.try_local(
        Check {
            report: &mut report,
            check: |name: &str, value: u8| assert_eq!((name, value), ("renamed", 7)),
        },
        Policy::new(
            |event| {
                assert!(
                    matches!(event, Reclaimed::Shutdown),
                    "unexpected operation: {event:?}"
                );
            },
            recover,
        ),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("renamed runtime");
            result
                .ok()
                .expect("completed renamed runtime")
                .expect("renamed fiber runtime completed")
                .expect("renamed fibers completed")
        },
    );
    assert!(Application::<'static>::INNER_ROUTE == route::Id::fixed::<17>());
    assert!(Selected::<'static>::INNER_ROUTE == route::Id::fixed::<17>());
    assert!(Components::<'static>::INNER_ROUTE == route::Id::fixed::<17>());
}

fn recover(failure: Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}
