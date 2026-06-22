#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub ring_entries: u32,
    pub cq_entries: u32,
    pub(super) fixed_file_slots: u32,
    pub(super) accept_slots: u32,
    pub(super) provided_buf_entries: u16,
    pub(super) provided_buf_len: usize,
    pub(super) defer_taskrun: bool,
}

impl Config {
    const IORING_MAX_ENTRIES: u32 = 32768;

    fn sized_sq(max_conn: u32, outbound_reserve: u32) -> u32 {
        max_conn
            .saturating_add(outbound_reserve)
            .next_power_of_two()
            .clamp(64, Self::IORING_MAX_ENTRIES)
    }
}

impl crate::backend::DriverConfig for Config {
    fn for_profile<P: crate::backend::profile::Profile>() -> Self {
        Self {
            ring_entries: P::RING_ENTRIES,
            cq_entries: P::CQ_ENTRIES,
            fixed_file_slots: P::FIXED_FILE_SLOTS,
            accept_slots: P::FIXED_FILE_SLOTS.saturating_sub(P::OUTBOUND_RESERVE),
            provided_buf_entries: P::PROVIDED_BUF_ENTRIES,
            provided_buf_len: P::PROVIDED_BUF_LEN,
            defer_taskrun: P::DEFER_TASKRUN,
        }
    }

    fn for_tcp_profile<P: crate::backend::profile::Profile>(max_conn: usize) -> Self {
        let max_conn = max_conn as u32;
        let accept_slots = max_conn.min(P::FIXED_FILE_SLOTS.saturating_sub(P::OUTBOUND_RESERVE));
        let buf_len_ratio = (P::PROVIDED_BUF_LEN / 4096).max(1);
        let raw_entries = (accept_slots as usize).max(2048) * 4;
        Self {
            ring_entries: Self::sized_sq(accept_slots, P::OUTBOUND_RESERVE),
            cq_entries: P::CQ_ENTRIES,
            fixed_file_slots: P::FIXED_FILE_SLOTS,
            accept_slots,
            provided_buf_entries: (raw_entries.min(32768) / buf_len_ratio).max(1024) as u16,
            provided_buf_len: P::PROVIDED_BUF_LEN,
            defer_taskrun: P::DEFER_TASKRUN,
        }
    }

    fn for_quic_udp(provided_buf_entries: u32, provided_buf_len: u32) -> Self {
        Self {
            ring_entries: 256,
            cq_entries: 1024,
            fixed_file_slots: 16,
            accept_slots: 0,
            provided_buf_entries: provided_buf_entries.min(u16::MAX as u32) as u16,
            provided_buf_len: provided_buf_len as usize,
            defer_taskrun: false,
        }
    }
}
