use std::{mem, num::NonZero};

use dope_core::driver::{
    receive::credit::error,
    route::{KeyTag, table::conn},
};
use dope_manifold::client::{
    close::{Capacity, Directive, LocalOrigin, Policy, Reason},
    conn::CloseRequest,
    deadline,
};
use dope_net::link::event;
use o3::{
    alloc,
    buffer::resident::{staging::reserve, write},
    depot::queue::tagged::entry,
};

const REASONS: [Reason; 25] = [
    Reason::Local(LocalOrigin::ApiRequest),
    Reason::Local(LocalOrigin::SideComplete),
    Reason::Local(LocalOrigin::OperationCancelled),
    Reason::Local(LocalOrigin::Superseded),
    Reason::Local(LocalOrigin::ScheduledRotation),
    Reason::Local(LocalOrigin::RuntimeShutdown),
    Reason::Local(LocalOrigin::ResourceFailure),
    Reason::Capacity(Capacity::IngressStaging),
    Reason::Capacity(Capacity::IngressRetention),
    Reason::Capacity(Capacity::Transport),
    Reason::EndpointRetired,
    Reason::Transport,
    Reason::Timeout(deadline::Kind::Connect),
    Reason::Timeout(deadline::Kind::Inbound),
    Reason::Timeout(deadline::Kind::Send),
    Reason::Timeout(deadline::Kind::Lifetime),
    Reason::Protocol,
    Reason::Remote,
    Reason::Receive(event::ParkError::Credit(error::Arm::Stale(error::Stale {
        expected: NonZero::<u64>::MIN,
        observed: error::Generation::Retired(NonZero::<u64>::MIN),
    }))),
    Reason::Receive(event::ParkError::Credit(error::Arm::Busy)),
    Reason::Receive(event::ParkError::Credit(error::Arm::Detached)),
    Reason::Receive(event::ParkError::Register(entry::Error::Index {
        index: usize::MAX,
        capacity: conn::Capacity::fixed::<7>(),
    })),
    Reason::Receive(event::ParkError::Full),
    Reason::Receive(event::ParkError::Occupied),
    Reason::Receive(event::ParkError::Unavailable),
];
const POLICIES: [Policy; 4] = [
    Policy::Reconnect,
    Policy::Permanent,
    Policy::ImmediateReconnect,
    Policy::ImmediatePermanent,
];

#[test]
fn close_request_layout() {
    assert_eq!(
        mem::size_of::<CloseRequest<'static, 'static, KeyTag<0>>>(),
        2 * mem::size_of::<&mut ()>(),
    );
}

#[test]
fn directives_are_unique() {
    let mut values = Vec::with_capacity(REASONS.len() * 4);
    for reason in REASONS {
        values.extend([
            Directive::reconnect(reason),
            Directive::permanent(reason),
            Directive::cut_retry(reason),
            Directive::cut_final(reason),
        ]);
    }
    assert_eq!(values.len(), REASONS.len() * POLICIES.len());
    for (index, value) in values.iter().enumerate() {
        assert!(value.reason() == REASONS[index / POLICIES.len()]);
        assert!(value.policy() == POLICIES[index % POLICIES.len()]);
        for distinct in &values[index + 1..] {
            assert!(value != distinct);
        }
    }
}

#[test]
fn retains_spill_cause() {
    use dope_manifold::app::receive;
    use o3::buffer::{resident, write::consume};

    let result = consume::Error::check(9, 4);
    if let Err(capacity) = result {
        let source = write::Cause::Shared(capacity);
        let error = resident::AllocError::Exhausted(alloc::Exhausted);
        let cause = reserve::Error::Spill { source, error };
        let failure = receive::Failure::Staging(cause);
        let reason = Reason::from(failure);
        assert!(reason == Reason::Ingress(failure));
        for policy in POLICIES {
            let value = Directive::with_policy(reason, policy);
            assert!(value.reason() == reason);
            assert!(value.policy() == policy);
        }
    } else {
        assert!(
            result.is_err(),
            "nine required bytes exceed four available bytes"
        );
    }
}
