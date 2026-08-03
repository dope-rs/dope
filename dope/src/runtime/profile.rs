use std::time::Duration;

use crate::driver::profile::DriverProfile;

/// Application-level timing and admission policy layered on a driver profile.
pub trait RuntimeProfile: DriverProfile {
    const HYBRID_PARK: bool = false;
    const IDLE_WINDOW: Duration;
    const SEND_DEADLINE: Option<Duration> = None;
    const USER_TIMEOUT: Option<Duration> = None;
    const ABS_CONN_AGE: Option<Duration> = None;
    const PER_IP_CAP: u32 = 0;
}

/// Balanced defaults suitable for general production services.
#[derive(Debug)]
pub struct Balanced;

impl DriverProfile for Balanced {
    const RING_ENTRIES: u32 = 8192;
    const FIXED_FILE_SLOTS: u32 = 65535;
    const OUTBOUND_RESERVE: u32 = 1024;
    const RECV_BUF_ENTRIES: u16 = 1024;
    const RECV_BUF_LEN: usize = 8192;
}

impl RuntimeProfile for Balanced {
    const IDLE_WINDOW: Duration = Duration::from_secs(30);
    const SEND_DEADLINE: Option<Duration> = Some(Duration::from_secs(5));
    const USER_TIMEOUT: Option<Duration> = Some(Duration::from_secs(5));
    const ABS_CONN_AGE: Option<Duration> = Some(Duration::from_secs(300));
    const PER_IP_CAP: u32 = 256;
}

/// Favors low tail latency over batching and resource density.
#[derive(Debug)]
pub struct LowLatency;

impl DriverProfile for LowLatency {
    const RING_ENTRIES: u32 = 2048;
    const DEFER_TASKRUN: bool = false;
}

impl RuntimeProfile for LowLatency {
    const IDLE_WINDOW: Duration = Duration::from_secs(10);
    const SEND_DEADLINE: Option<Duration> = Some(Duration::from_secs(2));
    const USER_TIMEOUT: Option<Duration> = Some(Duration::from_secs(2));
    const ABS_CONN_AGE: Option<Duration> = Some(Duration::from_secs(120));
    const PER_IP_CAP: u32 = 256;
}

/// Favors sustained throughput and larger transfer batches.
#[derive(Debug)]
pub struct Throughput;

impl DriverProfile for Throughput {
    const RING_ENTRIES: u32 = 4096;
    const RECV_BUF_LEN: usize = 64 * 1024;
}

impl RuntimeProfile for Throughput {
    const HYBRID_PARK: bool = true;
    const IDLE_WINDOW: Duration = Duration::from_secs(60);
    const SEND_DEADLINE: Option<Duration> = Some(Duration::from_secs(30));
    const USER_TIMEOUT: Option<Duration> = Some(Duration::from_secs(30));
    const ABS_CONN_AGE: Option<Duration> = Some(Duration::from_secs(600));
    const PER_IP_CAP: u32 = 256;
}
