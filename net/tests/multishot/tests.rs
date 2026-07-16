use dope_core::driver::token::{SlotIndex, Token};
use dope_net::multishot::Multishot;

fn armed() -> (Multishot, Token) {
    let mut arm = Multishot::default();
    let ud = arm.begin(7, SlotIndex::new(0)).expect("begin");
    arm.settle(true);
    (arm, ud)
}

#[test]
fn completion_in_armed_requests_rearm() {
    let (mut arm, ud) = armed();
    assert!(arm.is_armed());
    assert!(arm.epoch_match(ud, SlotIndex::new(0)));
    arm.complete(false);
    assert!(!arm.is_armed());
    assert!(arm.needs_rearm());

    let (mut arm, ud) = armed();
    arm.quiesce();
    assert!(!arm.is_armed());
    assert!(arm.epoch_match(ud, SlotIndex::new(0)));
    arm.complete(false);
    assert!(!arm.needs_rearm());
    assert!(!arm.is_armed());

    let (mut arm, _) = armed();
    arm.complete(true);
    assert!(arm.is_armed());
    assert!(!arm.needs_rearm());
}
