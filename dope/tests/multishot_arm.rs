use dope::runtime::token::LocalIdx;
use dope::transport::multishot::Arm;

#[test]
fn completion_in_armed_requests_rearm() {
    let mut arm = Arm::default();
    let ud = arm.begin(7, LocalIdx::new(0)).expect("begin");
    arm.settle(true);
    assert!(arm.is_armed());
    assert!(arm.epoch_match(ud, LocalIdx::new(0)));
    arm.on_completion(false);
    assert!(!arm.is_armed());
    assert!(arm.needs_rearm());
}

#[test]
fn quiesce_is_sticky_across_canceled_completion() {
    let mut arm = Arm::default();
    let ud = arm.begin(7, LocalIdx::new(0)).expect("begin");
    arm.settle(true);
    arm.quiesce();
    assert!(!arm.is_armed());
    assert!(arm.epoch_match(ud, LocalIdx::new(0)));
    arm.on_completion(false);
    assert!(!arm.needs_rearm());
    assert!(!arm.is_armed());
}

#[test]
fn multishot_more_keeps_armed() {
    let mut arm = Arm::default();
    let _ = arm.begin(7, LocalIdx::new(0)).expect("begin");
    arm.settle(true);
    arm.on_completion(true);
    assert!(arm.is_armed());
    assert!(!arm.needs_rearm());
}
