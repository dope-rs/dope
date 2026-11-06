use dope_manifold::client::port::{Plan, error};
use dope_net::link::egress::{Config, Credit, Entries, Resident};

#[test]
fn combines_alternative_receive_budgets() {
    use dope_core::driver::route::table::conn::Capacity;
    use dope_manifold::app::receive::limits::Budget;
    use o3::buffer::hold::limits::Bound;

    let budget = Budget::new(Bound::new(32), 0).max(Budget::new(Bound::new(0), 128));
    for (count, expected) in [(Capacity::fixed::<1>(), 128), (Capacity::fixed::<8>(), 256)] {
        assert_eq!(
            budget.capacity(count).expect("resident budget").into_raw(),
            expected
        );
    }
}

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
