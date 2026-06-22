use crate::backend::profile::Profile;

#[derive(Debug)]
pub struct Production;

impl Profile for Production {
    const RING_ENTRIES: u32 = 32768;

    const FIXED_FILE_SLOTS: u32 = 65535;
    const OUTBOUND_RESERVE: u32 = 1024;
    const PROVIDED_BUF_ENTRIES: u16 = 32768;
    const PROVIDED_BUF_LEN: usize = 8192;

    const IDLE_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);

    const SEND_DEADLINE: Option<std::time::Duration> = Some(std::time::Duration::from_secs(5));

    const USER_TIMEOUT: Option<std::time::Duration> = Some(std::time::Duration::from_secs(5));

    const ABS_CONN_AGE: Option<std::time::Duration> = Some(std::time::Duration::from_secs(300));

    const PER_IP_CAP: u32 = 256;
}
