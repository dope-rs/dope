//! I0 instrumentation: per-thread high-water-mark (HWM) counters for the buffer
//! classes whose sizing is reviewed in MEMORY_DESIGN.md. Thread-per-core, so the
//! storage is a plain `thread_local!` of `Cell` counters with NO atomics.
//!
//! Everything is gated behind the `mem-stats` cargo feature via a single
//! `cfg_select!`. When the feature is off, every public function is an empty
//! body operating on a zero-sized state and compiles to nothing (DCE), so the
//! hot-path call sites are zero-cost.
//!
//! The TLS counters (`tls_*`) have no storage of their own in dope-tls: that
//! crate calls these dope functions (behind its own `mem-stats` feature) so that
//! a single per-thread readout line covers every class, TLS included.

cfg_select! {
    feature = "mem-stats" => {
        use std::cell::Cell;

        #[derive(Default)]
        struct MemStats {
            provided_live: Cell<u32>,
            provided_hwm: Cell<u32>,
            enobufs: Cell<u64>,
            starved_rearm: Cell<u64>,
            arena_hwm: Cell<u32>,
            send_live: Cell<u32>,
            send_hwm: Cell<u32>,
            tls_egress_live: Cell<u32>,
            tls_egress_hwm: Cell<u32>,
            tls_recv_live: Cell<u32>,
            tls_recv_hwm: Cell<u32>,
            tls_send_live: Cell<u32>,
            tls_send_hwm: Cell<u32>,
        }

        thread_local! {
            static STATS: MemStats = MemStats::default();
        }

        fn bump(live: &Cell<u32>, hwm: &Cell<u32>) {
            let n = live.get() + 1;
            live.set(n);
            if n > hwm.get() {
                hwm.set(n);
            }
        }

        fn drop_one(live: &Cell<u32>) {
            live.set(live.get().saturating_sub(1));
        }

        pub fn provided_borrow() {
            STATS.with(|s| bump(&s.provided_live, &s.provided_hwm));
        }

        pub fn provided_release() {
            STATS.with(|s| drop_one(&s.provided_live));
        }

        pub fn enobufs_inc() {
            STATS.with(|s| s.enobufs.set(s.enobufs.get() + 1));
        }

        pub fn starved_rearm_inc() {
            STATS.with(|s| s.starved_rearm.set(s.starved_rearm.get() + 1));
        }

        /// Record the current live write-arena buffer count (`bufs.len() - free.len()`).
        pub fn arena_observe(live: usize) {
            STATS.with(|s| {
                let live = live as u32;
                if live > s.arena_hwm.get() {
                    s.arena_hwm.set(live);
                }
            });
        }

        pub fn send_borrow() {
            STATS.with(|s| bump(&s.send_live, &s.send_hwm));
        }

        pub fn send_release() {
            STATS.with(|s| drop_one(&s.send_live));
        }

        pub fn tls_egress_borrow() {
            STATS.with(|s| bump(&s.tls_egress_live, &s.tls_egress_hwm));
        }

        pub fn tls_egress_release() {
            STATS.with(|s| drop_one(&s.tls_egress_live));
        }

        pub fn tls_recv_borrow() {
            STATS.with(|s| bump(&s.tls_recv_live, &s.tls_recv_hwm));
        }

        pub fn tls_recv_release() {
            STATS.with(|s| drop_one(&s.tls_recv_live));
        }

        pub fn tls_send_borrow() {
            STATS.with(|s| bump(&s.tls_send_live, &s.tls_send_hwm));
        }

        pub fn tls_send_release() {
            STATS.with(|s| drop_one(&s.tls_send_live));
        }

        /// Print one per-thread line of all HWMs + miss counters to stderr.
        /// Called on driver/executor shutdown.
        pub fn dump() {
            STATS.with(|s| {
                let cpu = current_cpu();
                eprintln!(
                    "mem-stats[cpu={cpu}] provided_hwm={} enobufs={} starved_rearm={} \
arena_hwm={} send_hwm={} tls_egress_hwm={} tls_recv_hwm={} tls_send_hwm={}",
                    s.provided_hwm.get(),
                    s.enobufs.get(),
                    s.starved_rearm.get(),
                    s.arena_hwm.get(),
                    s.send_hwm.get(),
                    s.tls_egress_hwm.get(),
                    s.tls_recv_hwm.get(),
                    s.tls_send_hwm.get(),
                );
            });
        }

        fn current_cpu() -> i32 {
            cfg_select! {
                target_os = "linux" => {
                    // SAFETY: sched_getcpu takes no arguments and only reads the
                    // calling thread's current CPU; -1 on failure is reported verbatim.
                    unsafe { libc::sched_getcpu() }
                }
                _ => -1
            }
        }
    }
    _ => {
        pub fn provided_borrow() {}
        pub fn provided_release() {}
        pub fn enobufs_inc() {}
        pub fn starved_rearm_inc() {}
        pub fn arena_observe(_live: usize) {}
        pub fn send_borrow() {}
        pub fn send_release() {}
        pub fn tls_egress_borrow() {}
        pub fn tls_egress_release() {}
        pub fn tls_recv_borrow() {}
        pub fn tls_recv_release() {}
        pub fn tls_send_borrow() {}
        pub fn tls_send_release() {}
        pub fn dump() {}
    }
}
