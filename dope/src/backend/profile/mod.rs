mod production;
mod tail;
mod throughput;

pub use production::Production;
pub use tail::Tail;
pub use throughput::Throughput;

pub trait Profile: 'static {
    const RING_ENTRIES: u32;
    const CQ_ENTRIES: u32 = 65536;
    const HYBRID_PARK: bool = false;
    const DEFER_TASKRUN: bool = true;

    const FIXED_FILE_SLOTS: u32 = 65535;
    const OUTBOUND_RESERVE: u32 = 256;
    const PROVIDED_BUF_ENTRIES: u16 = 4096;
    const PROVIDED_BUF_LEN: usize = 4096;

    const TASK_SLAB_CAPACITY: usize = 65536;

    const IDLE_WINDOW: std::time::Duration;

    const SEND_DEADLINE: Option<std::time::Duration> = None;

    const USER_TIMEOUT: Option<std::time::Duration> = None;

    const ABS_CONN_AGE: Option<std::time::Duration> = None;

    const PER_IP_CAP: u32 = 0;
}
