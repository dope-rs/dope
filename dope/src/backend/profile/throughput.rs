use crate::backend::profile::Profile;

#[derive(Debug)]
pub struct Throughput;

impl Profile for Throughput {
    const RING_ENTRIES: u32 = 4096;
    const HYBRID_PARK: bool = true;

    const PROVIDED_BUF_LEN: usize = 64 * 1024;

    const IDLE_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);

    const SEND_DEADLINE: Option<std::time::Duration> = Some(std::time::Duration::from_secs(30));

    const USER_TIMEOUT: Option<std::time::Duration> = Some(std::time::Duration::from_secs(30));

    const ABS_CONN_AGE: Option<std::time::Duration> = Some(std::time::Duration::from_secs(600));
}
