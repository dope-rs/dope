use dope_core::driver::{
    retained::flight::{Capacity, ScaleError},
    route,
    route::table::conn,
};

#[test]
fn reports_scale_product() {
    let cap = conn::Capacity::fixed::<2>();
    let observed = Capacity::connections::<{ usize::MAX }>(cap);

    assert!(matches!(
        observed,
        Err(ScaleError::Product {
            capacity,
            factor: usize::MAX,
        }) if capacity == cap
    ));
}

#[test]
fn reports_scale_limit() {
    let cap = conn::Capacity::fixed::<{ route::SLOT_MASK as usize }>();
    let observed = Capacity::connections::<2>(cap);

    assert!(matches!(
        observed,
        Err(ScaleError::Limit { actual, maximum })
            if actual == 2 * cap.count().get() as usize && maximum < actual
    ));
}
