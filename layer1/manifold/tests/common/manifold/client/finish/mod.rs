use dope_manifold::client::close::Reason;

pub mod close;
pub mod half;
pub mod lifetime;
pub mod shutdown;

#[derive(Debug)]
pub enum Error {
    NonemptyOutput,
    Closed {
        conn: usize,
        closed: usize,
    },
    Flush {
        connected: u32,
        closed: u32,
        empty: bool,
    },
    HalfSent {
        connected: u32,
        pending: bool,
    },
    HalfRetired {
        reason: Reason,
        connected: u32,
        closed: u32,
        issued: bool,
        sent: bool,
        action: half::Action,
    },
    Epoch {
        connected: u32,
        closed: u32,
    },
    ShutdownRetired {
        reason: Reason,
        connected: u32,
    },
    Shutdown {
        connected: u32,
        closed: u32,
    },
}
