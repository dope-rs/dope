use std::cell;

use dope_manifold::client::close::Reason;

mod flight;
pub mod lane;
mod payload;
pub mod profile;
pub mod reject;
pub mod requests;
pub mod wait;
pub mod wire;

pub use flight::Flight;
pub use payload::Payload;

#[derive(Debug)]
pub enum Error {
    Borrow(cell::BorrowError),
    BorrowMut(cell::BorrowMutError),
    MissingPayload,
    FlushPayload(usize),
    SentPayload(usize),
    Cancel { bytes: usize, closed: usize },
    Bytes(Vec<u8>),
    Index { actual: usize, capacity: usize },
    Quarantine(Reason),
    Closed { actual: usize, maximum: usize },
    Unassigned(usize),
    Pending { index: usize, bytes: usize },
    Flush { pending: bool, empty: bool },
    AdmissionReady,
    AdmissionExhausted,
    AdmissionSaturated,
}
