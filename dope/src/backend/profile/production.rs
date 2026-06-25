use crate::backend::profile::Profile;

#[derive(Debug)]
pub struct Production;

impl Profile for Production {
    // Realistic SQ depth. 32768 (the io_uring max) over-reserves ring pages for
    // an in-flight op depth that never approaches it on a thread-per-core loop
    // that drains <=256 CQEs per tick. 8192 is conservative headroom over that
    // batch and over the per-thread conn count at the harness max (16384 conns /
    // 64c = 256 conns/thread). See MEMORY_DESIGN.md (ring realism).
    const RING_ENTRIES: u32 = 8192;

    // Fixed-file table CEILING (the maximum acceptable conns). The actually
    // registered table -- a kernel fd-index array AND a matching userspace park
    // arena -- is sized to the requested `max_conn` (accept_slots +
    // OUTBOUND_RESERVE) by `for_tcp_profile`, NOT to this const. So a small
    // deployment never pays for the ceiling and a large one is not capped below
    // it; only the requested conns are reserved. 65535 keeps the full conn range
    // available. See MEMORY_DESIGN.md (fixed-file realism).
    const FIXED_FILE_SLOTS: u32 = 65535;
    const OUTBOUND_RESERVE: u32 = 1024;
    // Provided-pool entries are derived from the recv HWM (DRAIN_BATCH), not from
    // PROVIDED_BUF_ENTRIES, by `Config::provided_entries`. This const is only the
    // fallback for the non-TCP `for_profile` path; keep it at the HWM-margin
    // count so even that path is realistic.
    const PROVIDED_BUF_ENTRIES: u16 = 1024;
    const PROVIDED_BUF_LEN: usize = 8192;

    const IDLE_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);

    const SEND_DEADLINE: Option<std::time::Duration> = Some(std::time::Duration::from_secs(5));

    const USER_TIMEOUT: Option<std::time::Duration> = Some(std::time::Duration::from_secs(5));

    const ABS_CONN_AGE: Option<std::time::Duration> = Some(std::time::Duration::from_secs(300));

    const PER_IP_CAP: u32 = 256;
}
