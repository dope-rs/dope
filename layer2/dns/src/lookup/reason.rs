use dope::net::link::event;

/// Why DNS issued a lookup.
#[derive(Clone, Copy)]
pub enum Reason {
    Startup,
    Scheduled,
    ConnectFailure(event::ConnectError),
    ConnectTimeout,
    TransportFailure,
    IdleFailure,
}
