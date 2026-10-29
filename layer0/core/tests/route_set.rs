use dope_core::driver::{route::KeyTag, settings::Routes};

#[test]
fn join_idempotent() {
    let route = Routes::singleton::<KeyTag<7>>();
    assert!(route.join(route) == route);
}
