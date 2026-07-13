use std::pin::pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use std::future::Future;

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
    let exec = Executor::new(cfg).expect("executor");
    let mut sess = exec.enter();

    let mut app = pin!(Clock {
        timer: Timer::new()
    });

    let timer_ptr: NonNull<Timer> = NonNull::from(&app.timer);
    // SAFETY: `app` is pinned for the whole test; the timer field never moves.
    let hold: Holding<'_, Timer> = unsafe { Holding::from_raw(timer_ptr) };

    let start = Instant::now();
    dope_extra::block_on(
        &mut sess,
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
    let exec = Executor::new(cfg).expect("executor");
    let sess = exec.enter();
    let slot = Box::pin(Parker::make_slot(
        sess.driver(),
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
    let exec = Executor::new(cfg).expect("executor");
    let sess = exec.enter();
    let slot = Box::pin(Parker::make_slot(
        sess.driver(),
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

#[test]
fn full_timer_does_not_livelock_and_release_wakes_starved() {
    let cfg = dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>();
    let exec = Executor::new(cfg).expect("executor");
    let sess = exec.enter();
    let armed = Box::pin(Parker::make_slot(
        sess.driver(),
        Token::new(0, LocalIdx::new(0), Epoch::INITIAL),
    ));
    let waiter = Box::pin(Parker::make_slot(
        sess.driver(),
        Token::new(0, LocalIdx::new(1), Epoch::INITIAL),
    ));
    let mut timer: Timer = Timer::with_capacity(1);
    let now = Instant::now();
    let held = timer
        .try_arm(now + Duration::from_secs(100), armed.wake_ref())
        .expect("arm fills the single slot");
    assert!(
        timer
            .try_arm(now + Duration::from_secs(100), waiter.wake_ref())
            .is_none(),
        "a full slab must refuse the arm"
    );

    timer.register_starved(waiter.wake_ref());
    let mut out = Vec::new();
    Parker::drain(sess.driver(), &mut out);
    assert!(
        out.is_empty(),
        "registering a starved waiter must NOT self-rewake (no busy-spin)"
    );

    timer.cancel(held);
    Parker::drain(sess.driver(), &mut out);
    let waiter_tok = Token::new(0, LocalIdx::new(1), Epoch::INITIAL);
    assert!(
        out.contains(&waiter_tok),
        "releasing a slot must wake the starved waiter"
    );
}

#[test]
fn far_future_sleep_arms_without_overflow() {
    let cfg = dope::DriverCfg::for_profile::<dope::runtime::profile::Throughput>();
    let exec = Executor::new(cfg).expect("executor");
    let sess = exec.enter();
    let app = pin!(Clock {
        timer: Timer::new()
    });
    let timer_ptr: NonNull<Timer> = NonNull::from(&app.timer);
    // SAFETY: `app` is pinned for the whole test; the timer field never moves.
    let hold: Holding<'_, Timer> = unsafe { Holding::from_raw(timer_ptr) };

    let slot = Box::pin(Parker::make_slot(
        sess.driver(),
        Token::new(0, LocalIdx::new(0), Epoch::INITIAL),
    ));
    let waker = slot.make_waker();
    let mut cx = Context::from_waker(&waker);

    let mut sleep = pin!(dope::fiber::Sleep::new(hold, Duration::MAX));
    assert!(
        matches!(sleep.as_mut().poll(&mut cx), Poll::Pending),
        "a Duration::MAX deadline must clamp instead of overflowing and stay pending"
    );
}
