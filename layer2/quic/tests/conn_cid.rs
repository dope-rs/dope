pub mod common;
use crate::common::e2e::life::{self, driver::Driver, identity::RouteId};
use common::suite::cid::contract::{ROUTES, Wire};
use std::num::NonZero;

#[test]
fn replays_lifecycle() {
    let topology = life::Topology::try_new(1, 1, &ROUTES)
        .expect("the fixture maps its sole transport and shard");
    let scenario =
        life::Scenario::new(topology, &life::CANONICAL_ACTIVE_STEPS, NonZero::<u16>::MIN);
    let mut driver = Wire::new().expect("the fixed lifecycle fixture must construct");
    let result = scenario.replay(&mut driver);
    assert!(
        matches!(result, life::Execution::Complete),
        "the canonical lifecycle trace must complete"
    );
    let delivery = driver
        .snapshot(RouteId::new(0))
        .expect("known route")
        .delivery();
    assert_eq!(delivery.accepted(), 4);
    assert_eq!(delivery.delivered(), 4);
    assert!(delivery.is_live());
    assert_eq!(driver.counts(), (1, 1));
}
