#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub ring_entries: u32,
    pub cq_entries: u32,
    pub(super) fixed_file_slots: u32,
    pub(super) accept_slots: u32,
    pub provided_buf_entries: u16,
    pub provided_buf_len: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ring_entries: 1024,
            cq_entries: 2048,
            fixed_file_slots: 65536,
            accept_slots: 65536,
            provided_buf_entries: 128,
            provided_buf_len: 4096,
        }
    }
}

impl crate::backend::DriverConfig for Config {
    fn for_profile<P: crate::backend::profile::Profile>() -> Self {
        Self {
            ring_entries: P::RING_ENTRIES,
            cq_entries: (P::RING_ENTRIES.saturating_mul(2)).next_power_of_two(),
            fixed_file_slots: P::FIXED_FILE_SLOTS,
            accept_slots: P::FIXED_FILE_SLOTS.saturating_sub(P::OUTBOUND_RESERVE),
            provided_buf_entries: P::PROVIDED_BUF_ENTRIES,
            provided_buf_len: P::PROVIDED_BUF_LEN,
        }
    }

    fn for_tcp_profile<P: crate::backend::profile::Profile>(max_conn: usize) -> Self {
        let accept_slots = (max_conn as u32)
            .min(P::FIXED_FILE_SLOTS.saturating_sub(P::OUTBOUND_RESERVE));
        Self {
            ring_entries: accept_slots
                .saturating_add(P::OUTBOUND_RESERVE)
                .next_power_of_two(),
            cq_entries: (accept_slots.saturating_mul(2).saturating_add(1024))
                .next_power_of_two(),
            fixed_file_slots: P::FIXED_FILE_SLOTS,
            accept_slots,
            provided_buf_entries: ((accept_slots as usize).max(2048) * 4).min(32768) as u16,
            provided_buf_len: P::PROVIDED_BUF_LEN,
        }
    }

    fn for_quic_udp(provided_buf_entries: u32, provided_buf_len: u32) -> Self {
        Self {
            ring_entries: 256,
            cq_entries: 1024,
            fixed_file_slots: 16,
            accept_slots: 0,
            provided_buf_entries: provided_buf_entries as u16,
            provided_buf_len: provided_buf_len as usize,
        }
    }
}
