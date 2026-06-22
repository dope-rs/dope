use std::pin::pin;
use std::ptr::NonNull;
use std::time::{Duration, Instant};

use dope::fiber::Holding;
use dope::manifold::timer::Timer;
use dope::runtime::park::Parker;
use dope::runtime::token::{Epoch, LocalIdx, Token};
use dope::{DriverConfig, Executor};

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Clock {
    #[pin]
    #[manifold]
    timer: Timer,
}

#[test]
fn sleep_expires_after_deadline() {
    let cfg = dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>();
    let mut exec = Executor::new(cfg).expect("executor");

    let mut app = pin!(Clock {
        timer: Timer::new()
    });

    let timer_ptr: NonNull<Timer> = NonNull::from(&app.timer);
    // SAFETY: `app` is pinned for the whole test; the timer field never moves.
    let hold: Holding<'_, Timer> = unsafe { Holding::from_raw(timer_ptr) };

    let start = Instant::now();
    dope_extra::block_on(
        &mut exec,
        app.as_mut(),
        dope::fiber::Fiber::new(async move {
            hold.sleep(Duration::from_millis(60)).await;
        }),
    );
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(55), "elapsed: {elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "elapsed: {elapsed:?}");
}

#[test]
fn earliest_tracks_min_deadline() {
    let cfg = dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>();
    let mut exec = Executor::new(cfg).expect("executor");
    let slot = Box::pin(Parker::make_slot(
        exec.driver_mut(),
        Token::new(0, LocalIdx::new(0), Epoch::INITIAL),
    ));
    let wake = slot.wake_ref();
    let mut timer: Timer = Timer::new();
    assert!(timer.earliest().is_none());
    let now = Instant::now();
    timer
        .try_arm(now + Duration::from_secs(10), wake)
        .expect("arm");
    timer
        .try_arm(now + Duration::from_secs(2), wake)
        .expect("arm");
    timer
        .try_arm(now + Duration::from_secs(5), wake)
        .expect("arm");
    let earliest = timer.earliest().expect("non-empty");
    assert!(earliest <= now + Duration::from_secs(2));
}

#[test]
fn expire_fires_due_entries_only() {
    let cfg = dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>();
    let mut exec = Executor::new(cfg).expect("executor");
    let slot = Box::pin(Parker::make_slot(
        exec.driver_mut(),
        Token::new(0, LocalIdx::new(0), Epoch::INITIAL),
    ));
    let wake = slot.wake_ref();
    let mut timer: Timer = Timer::new();
    let now = Instant::now();
    let due = timer
        .try_arm(now - Duration::from_secs(1), wake)
        .expect("arm");
    let pending = timer
        .try_arm(now + Duration::from_secs(100), wake)
        .expect("arm");
    timer.expire(now);
    assert!(timer.is_fired(due));
    assert!(!timer.is_fired(pending));
}
