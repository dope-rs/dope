use dope_net::ListenerTransport;
use dope_net::link::egress::config;

#[derive(Clone, Debug)]
pub struct Config<T: ListenerTransport> {
    pub max_connections: usize,
    pub bind: T::Addr,
    pub backlog: i32,
    pub stream: T::StreamConfig,
    pub transport: T::ListenerConfig,
    pub egress: config::Config,
}
