mod affinity;
mod alloc;
mod fiber;
mod file;
mod panic;
mod peer;
mod rt;

use std::time::Duration;

pub use affinity::{not_send, not_sync, not_unpin, require_send};
pub use alloc::{TrackingAlloc, allocations_during};
pub use fiber::{
    Gate, drive, drop_pending, poll_ready, poll_until_ready, poll_with_slot, pump_events,
    run_until, with_context,
};
pub use file::TempFile;
pub use panic::{
    CountDrop, OrderedDrop, assert_panics_with, assert_unwinds, counter, expect_abort, respawn_self,
};
pub use peer::{
    connect, connect_with_read_timeout, pattern, read_all, request_reply, reserve_addr, spawn_peer,
};
pub use rt::{
    Plain, TcpConfig, Wired, drain_tokens, exec, exec_for, file_exec, listener_exec, open_listener,
    quic_exec, tcp_host, throughput_cfg, tok, with_driver, with_session, with_session_for,
};

pub const GUARD: Duration = Duration::from_secs(5);
