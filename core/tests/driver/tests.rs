use std::pin::pin;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dope_core::backend::{Backend, Sqe};
use dope_core::driver;
use dope_core::driver::completion::Completion;
use dope_core::driver::ext::DriverExt;
use dope_core::driver::profile::DriverProfile;
use dope_core::driver::route::Route;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::{
    EPOCH_MASK, Epoch, KIND_SHIFT, KeyTag, SLOT_MASK, SlotIndex, Token, TokenCapacity,
    TokenCellSlab, TokenSlab, kind,
};
use dope_core::driver::{Driver, DriverRef, OutboundReservation};
use dope_core::io::Event;
use dope_core::platform::Platform;
use dope_core::platform::snapshot::{Mismatch, Snapshot};
use dope_test::{throughput_cfg, with_driver};

const ROUTE: u8 = 7;

fn target() -> Token {
    Token::new(ROUTE, SlotIndex::ZERO, Epoch::INITIAL)
}

#[test]
fn raw_tokens_require_a_kind_independent_target() {
    assert!(Token::try_from_raw(0).is_none());
    assert!(Token::try_from_raw(1 << KIND_SHIFT).is_none());

    let target = target();
    assert_eq!(target.epoch(), Some(Epoch::INITIAL));
    let tagged = Token::try_from_raw(target.with_kind(kind::SEND).raw()).expect("valid target");
    assert_eq!(tagged.with_kind(0), target);

    assert_eq!(Token::framework(SlotIndex::ZERO).epoch(), None);
    assert!(Epoch::new(0).is_none());
}

#[test]
fn slot_indices_validate_at_the_raw_boundary() {
    let first_overflow = SLOT_MASK as u32 + 1;
    assert!(SlotIndex::try_new(SLOT_MASK as u32).is_some());
    assert!(SlotIndex::try_new(first_overflow).is_none());

    let indexed = TokenCapacity::new(SLOT_MASK as usize).expect("sentinel capacity");
    assert_eq!(indexed.sentinel(), SlotIndex::try_new(SLOT_MASK as u32));
    assert_eq!(
        indexed.slots().next_back(),
        SlotIndex::try_new(SLOT_MASK as u32 - 1)
    );

    let full = TokenCapacity::new(SLOT_MASK as usize + 1).expect("full token capacity");
    assert!(full.sentinel().is_none());
    assert_eq!(
        full.slots().next_back(),
        SlotIndex::try_new(SLOT_MASK as u32)
    );
}

#[test]
fn epochs_advance_without_crossing_the_encoded_range() {
    assert_eq!(Epoch::INITIAL.next(), Epoch::new(2));
    assert_eq!(Epoch::MAX.raw(), EPOCH_MASK as u32);
    assert_eq!(Epoch::MAX.next(), None);
}

#[test]
fn driver_rejects_unencodable_fixed_file_layouts() {
    let mut oversized = driver::Config::for_quic_udp(1, 8);
    oversized.fixed_file_slots = SLOT_MASK as u32 + 2;
    assert!(matches!(
        Driver::new(oversized),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
    ));

    let mut inverted = driver::Config::for_quic_udp(1, 8);
    inverted.accept_slots = inverted.fixed_file_slots + 1;
    assert!(matches!(
        Driver::new(inverted),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
    ));
}

#[test]
fn token_slabs_issue_bounded_keys() {
    type Tag = KeyTag<ROUTE, { kind::SEND }>;

    let capacity = TokenCapacity::new(1).expect("bounded capacity");
    let mut slab = TokenSlab::<_, Tag>::with_capacity(capacity);
    let key = slab.insert(7).expect("slot");
    let token = Token::from_key(key);
    assert_eq!(token.slot(), SlotIndex::ZERO);
    assert_eq!(token.kind(), kind::SEND);
    let parts = token.parts::<Tag>().expect("typed parts");
    assert_eq!(Token::from_parts(parts), token);
    assert_eq!(slab.get(key), Some(&7));

    let cells = TokenCellSlab::<_, Tag>::with_capacity(capacity);
    let key = cells.insert(9).expect("slot");
    assert_eq!(Token::from_key(key).slot(), SlotIndex::ZERO);
    assert_eq!(cells.update(key, |value| *value), Some(9));

    let overflow = SLOT_MASK as usize + 2;
    assert!(TokenCapacity::new(overflow).is_none());
}

#[test]
fn quiesce_batch_reports_empty_and_targeted_batches() {
    with_driver(|mut driver| {
        let empty = driver.quiesce_batch().finish();
        assert!(!empty.has_targets());
        assert!(!empty.needs_poison());

        let mut quiesce = driver.quiesce_batch();
        quiesce.cancel(target());
        let targeted = quiesce.finish();
        assert!(targeted.has_targets());
        assert_eq!(targeted.needs_poison(), cfg!(target_os = "linux"));
    });
}

fn outbound_base<'d>(reservation: &OutboundReservation<'d>, driver: DriverRef<'d>) -> u32 {
    reservation
        .slot(SlotIndex::ZERO)
        .expect("non-empty reservation")
        .bind(driver)
        .index()
}

type TimerSpec = <Backend as Platform>::TimerSpec;

fn spec() -> &'static TimerSpec {
    static SPEC: OnceLock<TimerSpec> = OnceLock::new();
    SPEC.get_or_init(|| TimerSpec::from(Duration::from_millis(10)))
}

#[test]
fn interval_emits_repeated_completions_and_can_be_cancelled() {
    with_driver(|mut driver| {
        driver
            .push(Sqe::interval(spec(), target()))
            .expect("arm interval");

        let deadline = Instant::now() + Duration::from_millis(500);
        let mut ticks = 0;
        let mut completions = [const { None }; 16];
        while ticks < 2 && Instant::now() < deadline {
            driver
                .wait(Some(Duration::from_millis(50)))
                .expect("wait interval");
            let n = driver.drain(&mut completions);
            ticks += completions[..n]
                .iter()
                .filter(|event| {
                    event.as_ref().is_some_and(|event| {
                        matches!(
                            event,
                            Event::Timer(token, status)
                                if token.same_target(target()) && !status.is_cancelled()
                        )
                    })
                })
                .count();
            completions[..n].fill_with(|| None);
        }
        assert!(ticks >= 2, "interval produced only {ticks} completion(s)");

        driver
            .push(Sqe::cancel(target(), kind::TIMER))
            .expect("cancel interval");

        #[cfg(target_os = "linux")]
        {
            let deadline = Instant::now() + Duration::from_millis(500);
            let mut cancelled = false;
            while !cancelled && Instant::now() < deadline {
                driver
                    .wait(Some(Duration::from_millis(50)))
                    .expect("wait cancellation");
                let n = driver.drain(&mut completions);
                cancelled = completions[..n].iter().any(|event| {
                    event.as_ref().is_some_and(|event| {
                        matches!(
                            event,
                            Event::Timer(token, status)
                                if token.same_target(target()) && status.is_cancelled()
                        )
                    })
                });
                completions[..n].fill_with(|| None);
            }
            assert!(cancelled, "interval cancellation did not complete");
        }
    });
}

type Case = (&'static str, fn(&mut Snapshot), fn(Mismatch));

fn saturated_snapshot() -> Snapshot {
    let mut snap = Backend::snapshot().expect("detect");
    snap.rlimit_nofile = u64::MAX;
    snap.syncookies = true;
    snap.max_syn_backlog = u32::MAX;
    snap.somaxconn = u32::MAX;
    snap
}

#[test]
fn snapshot_detect_ok() {
    let snap = Backend::snapshot().expect("detect");
    assert!(snap.rlimit_nofile > 0);
    assert!(snap.somaxconn > 0);
}

#[test]
fn compat_check_baseline_cfg_passes_on_production_host() {
    let snap = Backend::snapshot().expect("detect");
    if !snap.syncookies && snap.max_syn_backlog < 4096 {
        return;
    }
    if snap.somaxconn < 4096 {
        return;
    }
    snap.check_slots(throughput_cfg().fixed_file_slots())
        .expect("baseline profile must pass on a properly tuned host");
}

#[test]
fn compat_check_rlimit_too_low_fails() {
    let cfg = throughput_cfg();
    let rows: [Case; 3] = [
        (
            "must reject 1-fd rlimit",
            |snap| snap.rlimit_nofile = 1,
            |err| match err {
                Mismatch::NoFileTooLow { rlimit, .. } => assert_eq!(rlimit, 1),
                other => panic!("expected NoFileTooLow, got {other:?}"),
            },
        ),
        (
            "must reject SYN-flood vulnerable host",
            |snap| {
                snap.syncookies = false;
                snap.max_syn_backlog = 128;
            },
            |err| match err {
                Mismatch::SynFloodVulnerable { backlog, .. } => assert_eq!(backlog, 128),
                other => panic!("expected SynFloodVulnerable, got {other:?}"),
            },
        ),
        (
            "must reject low somaxconn",
            |snap| snap.somaxconn = 128,
            |err| match err {
                Mismatch::SomaxconnTooLow { kernel, .. } => assert_eq!(kernel, 128),
                other => panic!("expected SomaxconnTooLow, got {other:?}"),
            },
        ),
    ];
    for (reject, degrade, verify) in rows {
        let mut snap = saturated_snapshot();
        degrade(&mut snap);
        verify(snap.check_slots(cfg.fixed_file_slots()).expect_err(reject));
    }
}

#[test]
fn mismatch_to_io_error() {
    let err: std::io::Error = Mismatch::NoFileTooLow {
        requested: 100,
        rlimit: 50,
    }
    .into();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    let msg = err.to_string();
    assert!(msg.contains("100"));
    assert!(msg.contains("50"));
}

#[test]
fn driver_does_not_raise_nofile() {
    let before = Backend::snapshot().expect("detect before").rlimit_nofile;
    let driver = Driver::new(driver::Config::for_quic_udp(1, 8)).expect("driver");
    let after = Backend::snapshot().expect("detect after").rlimit_nofile;
    assert_eq!(after, before);
    drop(driver);
}

#[test]
fn ready_batch_admission_is_fallible_and_atomic() {
    let mut config = driver::Config::for_quic_udp(1, 8);
    config.ready_slots = 2;
    let mut driver = pin!(Driver::new(config).expect("driver"));
    driver.as_mut().scope(|mut scope| {
        let access = scope.context();
        let reference = access.driver_ref();
        let held = reference.make_ready_slot(target()).expect("first lease");
        let error = match reference.make_ready_slots([target().with_kind(1), target().with_kind(2)])
        {
            Ok(_) => panic!("oversized ready batch was admitted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        drop(held);
        let batch = reference
            .make_ready_slots([target().with_kind(1), target().with_kind(2)])
            .expect("atomic failure must not consume a slot");
        assert_eq!(batch.len(), 2);
    });
}

#[test]
fn outbound_reservation_only_issues_slots_inside_its_range() {
    let mut driver = pin!(Driver::new(driver::Config::for_quic_udp(1, 8)).expect("driver"));
    driver.as_mut().scope(|mut scope| {
        let mut access = scope.context();
        let reservation = access.reserve_outbound(1).expect("outbound reservation");
        assert!(reservation.slot(SlotIndex::ZERO).is_some());
        assert!(reservation.slot(SlotIndex::from(1_u16)).is_none());
    });
}

#[test]
fn outbound_ranges_reuse_non_lifo_holes() {
    let mut config = driver::Config::for_quic_udp(1, 8);
    config.fixed_file_slots = 10;
    config.accept_slots = 2;
    let mut driver = pin!(Driver::new(config).expect("driver"));
    driver.as_mut().scope(|mut scope| {
        let mut access = scope.context();
        let first = access.reserve_outbound(3).expect("first");
        let second = access.reserve_outbound(2).expect("second");
        let third = access.reserve_outbound(1).expect("third");
        let reference = access.driver_ref();
        assert_eq!(
            (
                outbound_base(&first, reference),
                outbound_base(&second, reference),
                outbound_base(&third, reference),
            ),
            (7, 5, 4)
        );

        access.retire_outbound(second);
        let high = access.reserve_outbound(1).expect("high");
        let low = access.reserve_outbound(1).expect("low");
        assert_eq!(
            (
                outbound_base(&high, reference),
                outbound_base(&low, reference),
            ),
            (6, 5)
        );
    });
}

#[test]
fn singleton_outbound_reuse_takes_the_lowest_hole() {
    let mut config = driver::Config::for_quic_udp(1, 8);
    config.fixed_file_slots = 12;
    config.accept_slots = 2;
    let mut driver = pin!(Driver::new(config).expect("driver"));
    driver.as_mut().scope(|mut scope| {
        let mut access = scope.context();
        let highest = access.reserve_outbound(2).expect("highest");
        let upper_live = access.reserve_outbound(2).expect("upper live");
        let lowest = access.reserve_outbound(2).expect("lowest");
        let lower_live = access.reserve_outbound(2).expect("lower live");
        let reference = access.driver_ref();
        assert_eq!(
            (
                outbound_base(&highest, reference),
                outbound_base(&upper_live, reference),
                outbound_base(&lowest, reference),
                outbound_base(&lower_live, reference),
            ),
            (10, 8, 6, 4)
        );

        access.retire_outbound(highest);
        access.retire_outbound(lowest);
        let first = access.reserve_outbound(1).expect("first singleton");
        let second = access.reserve_outbound(1).expect("second singleton");
        let reclaimed_high = access.reserve_outbound(2).expect("high hole");
        assert_eq!(
            (
                outbound_base(&first, reference),
                outbound_base(&second, reference),
                outbound_base(&reclaimed_high, reference),
            ),
            (7, 6, 10)
        );
    });
}

#[test]
fn outbound_ranges_coalesce_back_into_bump_space() {
    let mut config = driver::Config::for_quic_udp(1, 8);
    config.fixed_file_slots = 10;
    config.accept_slots = 2;
    let mut driver = pin!(Driver::new(config).expect("driver"));
    driver.as_mut().scope(|mut scope| {
        let mut access = scope.context();
        let first = access.reserve_outbound(3).expect("first");
        let second = access.reserve_outbound(2).expect("second");
        let third = access.reserve_outbound(1).expect("third");
        access.retire_outbound(first);
        access.retire_outbound(second);
        access.retire_outbound(third);

        let reclaimed = access.reserve_outbound(8).expect("fully reclaimed");
        assert_eq!(outbound_base(&reclaimed, access.driver_ref()), 2);
    });
}

#[test]
fn route_transaction_rolls_back_until_committed() {
    let mut driver = pin!(Driver::new(driver::Config::for_quic_udp(1, 8)).expect("driver"));
    driver.as_mut().scope(|mut scope| {
        let mut access = scope.context();
        drop(Route::<ROUTE>::reserve_transaction(&mut access).expect("transaction"));

        let route = Route::<ROUTE>::reserve_transaction(&mut access)
            .expect("rolled-back route")
            .commit();
        let error = match Route::<ROUTE>::reserve(&mut access) {
            Ok(_) => panic!("committed route was released"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);

        route.release(&mut access);
        let route = Route::<ROUTE>::reserve(&mut access).expect("released route");
        route.release(&mut access);
    });
}

#[cfg(target_pointer_width = "64")]
#[test]
fn tcp_profile_saturates_oversized_connection_counts() {
    struct Profile;

    impl DriverProfile for Profile {
        const RING_ENTRIES: u32 = 64;
        const FIXED_FILE_SLOTS: u32 = 128;
        const OUTBOUND_RESERVE: u32 = 16;
    }

    let config = driver::Config::for_tcp_profile::<Profile>(u32::MAX as usize + 1);
    assert_eq!(config.fixed_file_slots(), Profile::FIXED_FILE_SLOTS);
}
