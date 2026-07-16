use dope_net::Transport;

#[derive(Clone, Debug)]
pub struct Config<T: Transport> {
    pub max_connections: usize,
    pub bind: T::Addr,
    pub backlog: i32,
    pub stream: T::StreamConfig,
    pub transport: T::ListenerConfig,
    pub egress: dope_net::link::egress::config::Config,
}
