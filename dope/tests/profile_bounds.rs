extern crate dope;
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
