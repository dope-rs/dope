use dope_net::link::egress;

pub trait Admission {
    const PER_IP_LIMIT: u32;
}

pub struct StandardAdmission;

impl Admission for StandardAdmission {
    const PER_IP_LIMIT: u32 = 256;
}

#[derive(Clone, Debug)]
pub struct Config<T: dope_net::ListenerTransport> {
    pub max_connections: usize,
    /// Fixed pinned direct-send slots. Queued slots are bounded by `egress`.
    pub direct_flights: usize,
    pub bind: T::Addr,
    /// Requested kernel listen depth; the operating system may cap it to its host limit.
    pub backlog: i32,
    pub stream: T::StreamConfig,
    pub transport: T::ListenerConfig,
    pub egress: egress::Config,
}
