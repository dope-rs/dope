use super::buffer::BufferId;
use super::region::InitializedRegion;

pub(crate) struct CompletedBuffer {
    pub(in crate::io::provided) id: BufferId,
    pub(in crate::io::provided) region: InitializedRegion,
}

impl CompletedBuffer {
    pub(crate) const fn new(id: BufferId, region: InitializedRegion) -> Self {
        Self { id, region }
    }
}
