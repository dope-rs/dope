use std::time::Duration;

use dope_manifold::service::health;

#[test]
fn rejects_zero_backoff() {
    assert_eq!(
        health::Plan::new(Duration::ZERO, Duration::from_secs(1)).err(),
        Some(health::Error::Base),
    );
}

#[test]
fn rejects_reversed_range() {
    assert_eq!(
        health::Plan::new(Duration::from_secs(2), Duration::from_secs(1)).err(),
        Some(health::Error::Order {
            base: Duration::from_secs(2),
            maximum: Duration::from_secs(1),
        }),
    );
}
