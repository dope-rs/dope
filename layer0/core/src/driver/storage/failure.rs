use dope_sys::ring::layout;

use crate::driver::{result, result::IoOperation, storage::Mapping};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Kernel {
        operation: IoOperation,
        cause: result::Kernel,
    },
    Mapping(Mapping),
    RingLayout(layout::Layout),
    Capability(result::Capability),
    Probe(result::ProbeError),
}
