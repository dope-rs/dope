use dope_manifold::client::port::{Plan, error};
use dope_net::link::egress::{Config, Credit, Entries, Resident};

#[test]
fn reports_zero_axes() {
    assert!(matches!(
        Plan::<()>::new(0, Config::DEFAULT),
        Err(error::Plan::Lanes {
            actual: 0,
            minimum: 1,
            ..
        })
    ));
    let empty = Config::shared(Credit::new(Entries::new(0), Resident::new(1)));
    assert!(matches!(
        Plan::<()>::new(1, empty),
        Err(error::Plan::Entries {
            actual: 0,
            minimum: 1,
        })
    ));
    assert!(Plan::<()>::new(1, Config::DEFAULT).is_ok());
}

#[test]
fn reports_lane_maximum() {
    let actual = u32::MAX as usize;
    let observed = Plan::<()>::new(actual, Config::DEFAULT);

    assert!(matches!(
        observed,
        Err(error::Plan::Lanes {
            actual: value,
            minimum: 1,
            maximum,
        }) if value == actual && maximum == actual - 1
    ));
}
