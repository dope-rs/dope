use dope_core::driver::result::SubmitError;
use dope_manifold::client::port::error::Open;
use dope_test::harness::observe;

pub mod dial;
pub mod open;
pub mod ports;
pub mod rejected;
pub mod resolve;

#[derive(Debug)]
pub enum Error {
    Received,
    Prepared(u32),
    Closed(u32),
    Drained(u32),
    DeferredCapacity,
    DeferredSubmit(SubmitError),
    Connected(bool),
    Preparation(bool),
    Closing(bool),
    Retired {
        live: bool,
        closed: u32,
    },
    CreditTarget {
        slot: u32,
        capacity: u32,
    },
    CreditRelease {
        received: usize,
        sent: usize,
    },
    Flush {
        ready: u32,
        empty: bool,
    },
    Quarantine(u32),
    Disconnect {
        ready: u32,
        peers: Result<usize, observe::Error>,
    },
    Activation {
        first: Result<(), Open>,
        second: Result<(), Open>,
    },
    InactivePort(usize),
    PendingCall,
}
