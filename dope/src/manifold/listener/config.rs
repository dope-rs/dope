use crate::transport::Transport;

#[derive(Clone, Debug)]
pub struct Config<T: Transport> {
    pub max_conn: usize,
    pub bind: T::Addr,
    pub backlog: i32,
    pub stream_opts: T::StreamOpts,
    pub listener_opts: T::ListenerOpts,
}
