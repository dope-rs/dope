use dope_manifold::client::close::Reason;
use dope_net::link::egress;
use o3::buffer::storage::Shared;

mod flight;
pub mod lane;
mod payload;
pub mod policy;
pub mod profile;
pub mod reject;
pub mod requests;
pub mod wait;
pub mod wire;

pub use flight::Flight;
pub use payload::Payload;

#[derive(Debug)]
pub enum Error {
    MissingPayload,
    FlushPayload(usize),
    SentPayload(usize),
    Cancel { bytes: usize, closed: usize },
    Bytes(Vec<u8>),
    Quarantine(Reason),
    Closed { actual: usize, maximum: usize },
    Unassigned(usize),
    Pending { index: usize, bytes: usize },
    Flush { pending: bool, empty: bool },
    AdmissionReady,
    AdmissionExhausted,
    AdmissionSaturated,
}

pub fn load(bytes: &[u8]) -> egress::NonEmpty<Shared> {
    let value = Shared::try_copy(bytes).expect("shared request payload");
    egress::Load::new(value)
        .try_nonempty()
        .expect("nonempty request payload")
}
