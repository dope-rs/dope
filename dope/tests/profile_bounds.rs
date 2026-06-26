use dope::runtime::profile::{Production, Profile};

#[test]
fn production_profile_bounds_connection_age() {
    assert!(
        Production::ABS_CONN_AGE.is_some(),
        "Production must cap absolute connection age so a dribbling client cannot pin a slot forever"
    );
    assert!(
        Production::SEND_DEADLINE.is_some(),
        "Production must cap the send deadline"
    );
}
