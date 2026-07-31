use std::mem::size_of;

use dope_core::io::fd::Fd;
use dope_net::link::raw::core::Core;

#[allow(dead_code)]
enum BaselinePhase {
    Open,
    Draining,
    Closing,
}

#[allow(dead_code)]
enum BaselineRecvArm {
    Disarmed,
    Armed { discard: bool },
    Exhausted,
}

#[allow(dead_code)]
struct BaselineCore<'d> {
    fd: Fd<'d>,
    recv: BaselineRecvArm,
    phase: BaselinePhase,
    send_in_flight: bool,
    aborted: bool,
    graceful_requested: bool,
    graceful_sealed: bool,
    kernel_discard: bool,
    discard_remaining: u64,
}

#[test]
fn send_completion_proof_uses_existing_core_padding() {
    assert_eq!(
        size_of::<Core<'static>>(),
        size_of::<BaselineCore<'static>>()
    );
}
