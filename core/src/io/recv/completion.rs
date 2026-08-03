use super::raw::Region;
use crate::backend::RecvBuffer;

pub(crate) struct Completion {
    pub(in crate::io::recv) buffer: RecvBuffer,
    pub(in crate::io::recv) region: Region,
}

impl Completion {
    pub(crate) const fn new(buffer: RecvBuffer, region: Region) -> Self {
        Self { buffer, region }
    }
}
