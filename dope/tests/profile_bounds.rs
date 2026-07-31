extern crate dope;
use std::time::{Duration, Instant};

use dope::runtime::__private::Deadline;
use dope::runtime::profile::{Balanced, RuntimeProfile};

#[test]
fn balanced_profile_bounds_connection_age() {
    assert!(
        Balanced::ABS_CONN_AGE.is_some(),
        "Balanced must cap absolute connection age so a dribbling client cannot pin a slot forever"
    );
    assert!(
        Balanced::SEND_DEADLINE.is_some(),
        "Balanced must cap the send deadline"
    );
}

#[test]
fn oversized_deadline_saturates_without_panicking() {
    let now = Instant::now();
    assert!(Deadline::after(now, Duration::MAX) >= now);
}
