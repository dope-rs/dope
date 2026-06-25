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
        Self {
            // Cap the SQ depth at the profile's realistic RING_ENTRIES. The SQ
            // need not hold one slot per conn: multishot recvs arm once and the
            // per-tick SQE burst is bounded by the drain batch + accept/outbound,
            // not the live conn count. See MEMORY_DESIGN.md (ring realism).
            ring_entries: Self::sized_sq(accept_slots, P::OUTBOUND_RESERVE).min(P::RING_ENTRIES),
            cq_entries: P::CQ_ENTRIES,
            // Register only what this deployment can use: the requested conns
            // (accept_slots) plus the outbound reserve. `FIXED_FILE_SLOTS` is the
            // ceiling that bounds accept_slots, not the table size -- sizing here
            // avoids allocating the full ceiling's kernel table + park arena when
            // max_conn is smaller. See MEMORY_DESIGN.md (fixed-file realism).
            fixed_file_slots: accept_slots.saturating_add(P::OUTBOUND_RESERVE),
            accept_slots,
            provided_buf_entries: crate::backend::provided_entries(
                accept_slots,
                P::PROVIDED_BUF_LEN,
                Self::IORING_MAX_ENTRIES,
            ),
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

    fn with_provided(mut self, len: usize, entries: u16) -> Self {
        crate::backend::apply_provided(
            len,
            entries,
            &mut self.provided_buf_len,
            &mut self.provided_buf_entries,
        );
        self
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use crate::backend::DriverConfig;
    use crate::backend::profile::{Production, Profile, Throughput};

    const MIB: usize = 1024 * 1024;

    fn pool_bytes(c: &Config) -> usize {
        c.provided_buf_entries as usize * c.provided_buf_len
    }

    // C3: the default (Production) provided pool lands at ~1024 entries
    // (K_BATCH=4 * DRAIN_BATCH=256, i.e. 4x the <=256 recv HWM), 8 MiB/thread for
    // the 8 KiB buffer. The old default was 32768 entries = 256 MiB/thread.
    #[test]
    fn production_pool_is_hwm_sized() {
        let prod = Config::for_tcp_profile::<Production>(1024);
        assert_eq!(prod.provided_buf_len, 8192);
        assert_eq!(prod.provided_buf_entries, 1024);
        assert_eq!(pool_bytes(&prod), 8 * MIB);
    }

    // C3: the pool is sized by the recv HWM (a multiple of DRAIN_BATCH), NOT by
    // the connection count -- it is FLAT from 512 to 16384 conns. This is the
    // property that kills the "2.8-3.1 GiB flat across conns" leaderboard cost.
    #[test]
    fn production_pool_flat_across_conns() {
        let want = 1024u16;
        for conns in [512usize, 1024, 4096, 8192, 16384] {
            let c = Config::for_tcp_profile::<Production>(conns);
            assert_eq!(
                c.provided_buf_entries, want,
                "pool must be flat in conn count (conns={conns})"
            );
        }
    }

    // Ring realism: the SQ depth is capped at the profile's realistic
    // RING_ENTRIES (8192), not blown up to the io_uring max (32768) by the conn
    // count, even at the harness conn ceiling.
    #[test]
    fn production_ring_is_capped() {
        assert_eq!(Production::RING_ENTRIES, 8192);
        let c = Config::for_tcp_profile::<Production>(16384);
        assert!(
            c.ring_entries <= Production::RING_ENTRIES,
            "ring {} must not exceed RING_ENTRIES {}",
            c.ring_entries,
            Production::RING_ENTRIES
        );
    }

    // Fixed-file realism: the registered table tracks the REQUESTED max_conn
    // (accept_slots + outbound reserve), not the FIXED_FILE_SLOTS ceiling. A
    // mid-size deployment reserves only what it can use...
    #[test]
    fn production_fixed_file_tracks_requested_conns() {
        let c = Config::for_tcp_profile::<Production>(16384);
        assert_eq!(c.fixed_file_slots, 16384 + Production::OUTBOUND_RESERVE);
        assert_eq!(c.accept_slots, 16384);
        // ...far below the ceiling: no full-ceiling table is allocated.
        assert!(c.fixed_file_slots < Production::FIXED_FILE_SLOTS);

        // A tiny deployment pays only for its conns, not the ceiling.
        let small = Config::for_tcp_profile::<Production>(1024);
        assert_eq!(small.fixed_file_slots, 1024 + Production::OUTBOUND_RESERVE);

        // ...but the ceiling is intact: a large request reaches the full range
        // (no 16384 regression), capped only by the fd-index ceiling.
        let big = Config::for_tcp_profile::<Production>(100_000);
        assert_eq!(Production::FIXED_FILE_SLOTS, 65535);
        assert_eq!(big.fixed_file_slots, 65535);
        assert_eq!(
            big.accept_slots,
            65535 - Production::OUTBOUND_RESERVE,
            "large deployments are not capped below the ceiling"
        );
    }

    // Throughput keeps its 64 KiB buffer for transfer profiles; the larger buffer
    // divides entries by its page ratio so total bytes stay bounded (1024 floor).
    #[test]
    fn throughput_keeps_large_buffer() {
        let tp = Config::for_tcp_profile::<Throughput>(1024);
        assert_eq!(tp.provided_buf_len, 64 * 1024);
        assert_eq!(tp.provided_buf_entries, 1024);
    }

    // M2: explicit override sets sizing without minting a profile; 0 is no-op.
    // Kept as a genuine config knob (not a benchmark-only switch).
    #[test]
    fn with_provided_override() {
        let base = Config::for_tcp_profile::<Production>(1024);
        let tuned = base.with_provided(8192, 2048);
        assert_eq!(tuned.provided_buf_len, 8192);
        assert_eq!(tuned.provided_buf_entries, 2048);
        assert_eq!(pool_bytes(&tuned), 16 * MIB);

        let len_only = base.with_provided(4096, 0);
        assert_eq!(len_only.provided_buf_len, 4096);
        assert_eq!(len_only.provided_buf_entries, base.provided_buf_entries);

        let entries_only = base.with_provided(0, 512);
        assert_eq!(entries_only.provided_buf_len, base.provided_buf_len);
        assert_eq!(entries_only.provided_buf_entries, 512);
    }
}
