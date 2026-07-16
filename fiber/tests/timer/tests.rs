use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use dope::driver::ready::ReadySlot;
use dope::manifold::timer::Timer;
use dope_fiber::{Batch, TimerExt, Waker};

use dope_test::{drain_tokens, poll_with_slot, tok, with_session};

#[test]
fn sleep_expires_after_deadline() {
    with_session(|mut sess| {
        let timer: Timer<'_, 0> = Timer::with_capacity(1, sess.driver());
        let slot = pin!(sess.driver().make_ready_slot(tok(0)));
        let mut sleep = pin!(timer.sleep(Duration::from_millis(60)));
        let start = Instant::now();
        assert!(poll_with_slot(&mut sess, slot.as_ref(), sleep.as_mut()).is_pending());
        std::thread::sleep(Duration::from_millis(60));
        timer.expire(Instant::now());
        assert!(poll_with_slot(&mut sess, slot.as_ref(), sleep.as_mut()).is_ready());
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(55), "elapsed: {elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "elapsed: {elapsed:?}");
    });
}

#[test]
fn earliest_tracks_min_deadline() {
    with_session(|sess| {
        let slot = pin!(sess.driver().make_ready_slot(tok(0)));
        let wake = Waker::from_ready(sess.driver(), slot.as_ref().key());
        let timer: Timer = Timer::with_capacity(3, sess.driver());
        assert!(timer.earliest().is_none());
        let now = Instant::now();
        timer
            .try_arm(now + Duration::from_secs(10), wake.completion())
            .expect("arm");
        timer
            .try_arm(now + Duration::from_secs(2), wake.completion())
            .expect("arm");
        timer
            .try_arm(now + Duration::from_secs(5), wake.completion())
            .expect("arm");
        let pending_min = timer
            .earliest()
            .expect("pending arms must be visible before flush");
        assert!(pending_min <= now + Duration::from_secs(2));
        timer.flush();
        let earliest = timer.earliest().expect("non-empty");
        assert!(earliest <= now + Duration::from_secs(2));
    });
}

#[test]
fn expire_fires_due_entries_only() {
    with_session(|sess| {
        let slot = pin!(sess.driver().make_ready_slot(tok(0)));
        let wake = Waker::from_ready(sess.driver(), slot.as_ref().key());
        let timer: Timer = Timer::with_capacity(2, sess.driver());
        let now = Instant::now();
        let due = timer
            .try_arm(now - Duration::from_secs(1), wake.completion())
            .expect("arm");
        let pending = timer
            .try_arm(now + Duration::from_secs(100), wake.completion())
            .expect("arm");
        timer.expire(now);
        assert!(timer.is_fired(due));
        assert!(!timer.is_fired(pending));
    });
}

#[test]
fn batch_timer_completions_wake_the_exact_children() {
    with_session(|mut sess| {
        let root = pin!(sess.driver().make_ready_slot(tok(0)));
        let timer: Timer = Timer::with_capacity(2, sess.driver());
        let mut batch = pin!(Batch::from_array([
            timer.sleep(Duration::from_millis(10)),
            timer.sleep(Duration::from_millis(10)),
        ]));

        assert!(poll_with_slot(&mut sess, root.as_ref(), batch.as_mut()).is_pending());
        std::thread::sleep(Duration::from_millis(15));
        timer.expire(Instant::now());
        assert_eq!(drain_tokens(sess.driver()), [tok(0)]);
        assert!(poll_with_slot(&mut sess, root.as_ref(), batch.as_mut()).is_ready());
    });
}

#[test]
fn full_timer_does_not_livelock_and_release_wakes_starved() {
    with_session(|mut sess| {
        let armed_slot = pin!(sess.driver().make_ready_slot(tok(0)));
        let first_slot = pin!(sess.driver().make_ready_slot(tok(1)));
        let second_slot = pin!(sess.driver().make_ready_slot(tok(2)));
        let third_slot = pin!(sess.driver().make_ready_slot(tok(3)));
        let timer: Timer = Timer::with_capacity(1, sess.driver());
        let mut held = Box::pin(timer.sleep(Duration::from_secs(100)));
        let mut first = Box::pin(timer.sleep(Duration::from_secs(100)));
        let mut second = Box::pin(timer.sleep(Duration::from_secs(100)));
        let mut third = Box::pin(timer.sleep(Duration::from_secs(100)));
        assert!(poll_with_slot(&mut sess, armed_slot.as_ref(), held.as_mut()).is_pending());
        for _ in 0..3 {
            assert!(poll_with_slot(&mut sess, first_slot.as_ref(), first.as_mut()).is_pending());
        }
        assert!(poll_with_slot(&mut sess, second_slot.as_ref(), second.as_mut()).is_pending());
        assert!(poll_with_slot(&mut sess, third_slot.as_ref(), third.as_mut()).is_pending());
        assert!(drain_tokens(sess.driver()).is_empty());
        drop(held);
        timer.flush();
        assert_eq!(drain_tokens(sess.driver()), [tok(1)]);
        drop(first);
        assert_eq!(drain_tokens(sess.driver()), [tok(2)]);
        assert!(poll_with_slot(&mut sess, second_slot.as_ref(), second.as_mut()).is_pending());
        drop(second);
        timer.flush();
        assert_eq!(drain_tokens(sess.driver()), [tok(3)]);
    });
}

#[test]
fn starved_sleep_keeps_its_earlier_deadline() {
    with_session(|mut sess| {
        let held_slot = pin!(sess.driver().make_ready_slot(tok(0)));
        let early_slot = pin!(sess.driver().make_ready_slot(tok(1)));
        let timer: Timer = Timer::with_capacity(1, sess.driver());
        let mut held = pin!(timer.sleep(Duration::from_secs(100)));
        let mut early = pin!(timer.sleep(Duration::from_millis(20)));
        assert!(poll_with_slot(&mut sess, held_slot.as_ref(), held.as_mut()).is_pending());
        assert!(poll_with_slot(&mut sess, early_slot.as_ref(), early.as_mut()).is_pending());
        let earliest = timer.earliest().expect("starved deadline");
        assert!(earliest <= Instant::now() + Duration::from_millis(20));
        std::thread::sleep(Duration::from_millis(25));
        timer.expire(Instant::now());
        assert_eq!(drain_tokens(sess.driver()), [tok(1)]);
        assert!(poll_with_slot(&mut sess, early_slot.as_ref(), early.as_mut()).is_ready());
    });
}

#[test]
fn far_future_sleep_arms_without_overflow() {
    with_session(|mut sess| {
        let timer: Timer = Timer::with_capacity(1, sess.driver());
        let slot = pin!(sess.driver().make_ready_slot(tok(0)));
        let mut sleep = pin!(dope_fiber::Sleep::new(&timer, Duration::MAX));
        assert!(
            matches!(
                poll_with_slot(&mut sess, slot.as_ref(), sleep.as_mut()),
                Poll::Pending
            ),
            "a Duration::MAX deadline must clamp instead of overflowing and stay pending"
        );
    });
}

#[test]
fn starved_tree_survives_rotations_and_arbitrary_cancellation() {
    with_session(|mut sess| {
        let timer: Timer = Timer::with_capacity(0, sess.driver());
        let slots = sess.driver().make_ready_slots((0..64u32).map(tok));
        let mut sleeps = Vec::new();
        for index in 0..64u32 {
            let slot = ReadySlot::get(slots.as_ref(), index as usize).unwrap();
            let mut sleep = Box::pin(timer.sleep(Duration::from_secs(u64::from(64 - index))));
            assert!(poll_with_slot(&mut sess, slot, sleep.as_mut()).is_pending());
            sleeps.push(Some(sleep));
        }
        assert!(timer.earliest().is_some());
        for step in 0..64usize {
            drop(sleeps[(step * 37) & 63].take());
        }
        assert!(timer.earliest().is_none());
    });
}
