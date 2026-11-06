use dope_manifold::client::close::{Directive, LocalOrigin, Policy, Reason};
use o3::{
    alloc,
    buffer::resident::{staging::reserve, write},
};

const POLICIES: [Policy; 4] = [
    Policy::Reconnect,
    Policy::Permanent,
    Policy::ImmediateReconnect,
    Policy::ImmediatePermanent,
];

#[test]
fn directives_preserve_reason_and_policy() {
    let reason = Reason::Local(LocalOrigin::ApiRequest);
    for (value, policy) in [
        (Directive::reconnect(reason), Policy::Reconnect),
        (Directive::permanent(reason), Policy::Permanent),
        (Directive::cut_retry(reason), Policy::ImmediateReconnect),
        (Directive::cut_final(reason), Policy::ImmediatePermanent),
    ] {
        assert_eq!(value.reason(), reason);
        assert_eq!(value.policy(), policy);
    }
}

#[test]
fn retains_spill_cause() {
    use dope_manifold::app::receive;
    use o3::buffer::{resident, write::consume};

    let capacity = consume::Error::check(9, 4).expect_err("capacity");
    let source = write::Cause::Shared(capacity);
    let error = resident::AllocError::Exhausted(alloc::Exhausted);
    let cause = reserve::Error::Spill { source, error };
    let failure = receive::Failure::Staging(cause);
    let reason = Reason::from(failure);
    assert_eq!(reason, Reason::Ingress(failure));
    for policy in POLICIES {
        let value = Directive::with_policy(reason, policy);
        assert_eq!(value.reason(), reason);
        assert_eq!(value.policy(), policy);
    }
}
