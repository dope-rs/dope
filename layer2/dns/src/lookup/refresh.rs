use dope::manifold::service::health;

use crate::lookup;

/// Compile-time mapping from service failures to DNS refresh reasons.
pub trait Policy {
    fn reason(cause: health::Cause) -> Option<lookup::Reason>;
}

/// Refreshes after connection establishment or transport failure.
pub struct Transport;

impl Policy for Transport {
    fn reason(cause: health::Cause) -> Option<lookup::Reason> {
        match cause {
            health::Cause::Connect(cause) => Some(lookup::Reason::ConnectFailure(cause)),
            health::Cause::ConnectTimeout => Some(lookup::Reason::ConnectTimeout),
            health::Cause::Transport => Some(lookup::Reason::TransportFailure),
            health::Cause::Idle | health::Cause::Protocol | health::Cause::Remote => None,
        }
    }
}

/// Refreshes whenever an endpoint becomes unavailable to request traffic.
pub struct Availability;

impl Policy for Availability {
    fn reason(cause: health::Cause) -> Option<lookup::Reason> {
        match cause {
            health::Cause::Idle => Some(lookup::Reason::IdleFailure),
            cause => Transport::reason(cause),
        }
    }
}
