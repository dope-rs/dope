use std::{mem::size_of, ptr};

use dope_core::{driver::route, io::fd::handles::Descriptor};
use dope_net::{link::Engine, wire::send::Vectored};

enum BaselinePhase {
    Open,
    Draining,
    Closing,
}

enum BaselineRecvArm {
    Disarmed,
    Armed,
    Exhausted,
}

struct BaselineEngine<'d> {
    fd: Descriptor<'d>,
    recv: BaselineRecvArm,
    phase: BaselinePhase,
    send_in_flight: bool,
    aborted: bool,
    graceful_requested: bool,
    graceful_sealed: bool,
    discard_remaining: u64,
    establish: Option<ptr::NonNull<()>>,
    establish_cancel: Option<route::Token>,
    establish_completion: route::Token,
    establish_phase: usize,
}

fn observe_baseline(engine: &BaselineEngine<'_>) {
    std::hint::black_box((
        &engine.fd,
        &engine.recv,
        &engine.phase,
        engine.send_in_flight,
        engine.aborted,
        engine.graceful_requested,
        engine.graceful_sealed,
        engine.discard_remaining,
        &engine.establish,
        engine.establish_cancel,
        engine.establish_completion,
        engine.establish_phase,
    ));
}

#[test]
fn establishment_does_not_exceed_the_explicit_authority_baseline() {
    std::hint::black_box([
        BaselinePhase::Open,
        BaselinePhase::Draining,
        BaselinePhase::Closing,
    ]);
    std::hint::black_box([
        BaselineRecvArm::Disarmed,
        BaselineRecvArm::Armed,
        BaselineRecvArm::Exhausted,
    ]);
    std::hint::black_box(observe_baseline as fn(&BaselineEngine<'_>));
    assert!(
        size_of::<Engine<'static>>() <= size_of::<BaselineEngine<'static>>(),
        "engine layout {} exceeded the explicit establishment authority baseline {}",
        size_of::<Engine<'static>>(),
        size_of::<BaselineEngine<'static>>(),
    );
}

#[test]
fn stable_source_caches_length_without_duplicate_descriptor_storage() {
    assert_eq!(size_of::<Vectored<'static>>(), 4 * size_of::<usize>());
}
