use crate::backend::profile::Profile;

#[derive(Debug)]
pub struct Tail;

impl Profile for Tail {
    const RING_ENTRIES: u32 = 2048;
    const DEFER_TASKRUN: bool = false;

    const TASK_SLAB_CAPACITY: usize = 16384;

    const IDLE_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);
}
