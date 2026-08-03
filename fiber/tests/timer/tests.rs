use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use dope::driver::ready::CompletionWaker;
use dope::driver::timer::Registration;
use dope_fiber::abi::batch::Batch;
use dope_fiber::sleep::TimerExt;
use dope_test::{drain_tokens, poll_with_slot, tok, with_session_timer_slots};

#[test]
fn sleep_expires_after_deadline() {
    with_session_timer_slots(1, |mut sess| {
        let timer = sess.driver_access().timer();
        let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let mut sleep = pin!(timer.sleep(Duration::from_millis(60)));
        let start = Instant::now();
        assert!(poll_with_slot(&mut sess, &slot, sleep.as_mut()).is_pending());
        std::thread::sleep(Duration::from_millis(60));
        timer.expire(sess.driver_access().region_token(), Instant::now());
        assert!(poll_with_slot(&mut sess, &slot, sleep.as_mut()).is_ready());
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(55), "elapsed: {elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "elapsed: {elapsed:?}");
    });
}

#[test]
fn completed_sleep_remains_ready() {
    with_session_timer_slots(0, |mut sess| {
        let timer = sess.driver_access().timer();
        let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let mut sleep = pin!(timer.sleep(Duration::ZERO));
        assert!(poll_with_slot(&mut sess, &slot, sleep.as_mut()).is_ready());
        assert!(poll_with_slot(&mut sess, &slot, sleep.as_mut()).is_ready());
    });
}

#[test]
fn earliest_tracks_min_deadline() {
    with_session_timer_slots(3, |mut sess| {
        let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let timer = sess.driver_access().timer();
        assert!(
            timer
                .earliest(sess.driver_access().region_token_ref())
                .is_none()
        );
        let now = Instant::now();
        let late = pin!(Registration::with_deadline(
            timer,
            now + Duration::from_secs(10),
        ));
        let early = pin!(Registration::with_deadline(
            timer,
            now + Duration::from_secs(2),
        ));
        let middle = pin!(Registration::with_deadline(
            timer,
            now + Duration::from_secs(5),
        ));
        assert!(
            late.as_ref()
                .poll(now, CompletionWaker::from_ready(sess.driver(), slot.key()))
                .is_pending()
        );
        assert!(
            early
                .as_ref()
                .poll(now, CompletionWaker::from_ready(sess.driver(), slot.key()))
                .is_pending()
        );
        assert!(
            middle
                .as_ref()
                .poll(now, CompletionWaker::from_ready(sess.driver(), slot.key()))
                .is_pending()
        );
        let pending_min = timer
            .earliest(sess.driver_access().region_token_ref())
            .expect("pending arms must be visible before flush");
        assert!(pending_min <= now + Duration::from_secs(2));
        timer.flush(sess.driver_access().region_token());
        let earliest = timer
            .earliest(sess.driver_access().region_token_ref())
            .expect("non-empty");
        assert!(earliest <= now + Duration::from_secs(2));
    });
}

#[test]
fn expire_fires_due_entries_only() {
    with_session_timer_slots(2, |mut sess| {
        let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let timer = sess.driver_access().timer();
        let now = Instant::now();
        let due = pin!(Registration::with_deadline(
            timer,
            now - Duration::from_secs(1),
        ));
        let pending = pin!(Registration::with_deadline(
            timer,
            now + Duration::from_secs(100),
        ));
        assert!(
            due.as_ref()
                .poll(
                    now - Duration::from_secs(2),
                    CompletionWaker::from_ready(sess.driver(), slot.key())
                )
                .is_pending()
        );
        assert!(
            pending
                .as_ref()
                .poll(now, CompletionWaker::from_ready(sess.driver(), slot.key()))
                .is_pending()
        );
        timer.expire(sess.driver_access().region_token(), now);
        assert!(
            due.as_ref()
                .poll(now, CompletionWaker::from_ready(sess.driver(), slot.key()))
                .is_ready()
        );
        assert!(
            pending
                .as_ref()
                .poll(now, CompletionWaker::from_ready(sess.driver(), slot.key()))
                .is_pending()
        );
    });
}

#[test]
fn cancellation_is_idempotent_and_releases_the_slot() {
    with_session_timer_slots(1, |mut sess| {
        let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let timer = sess.driver_access().timer();
        assert_eq!(timer.capacity(), 1);
        let now = Instant::now();
        let first = pin!(Registration::new(timer));
        first.as_ref().arm(
            now + Duration::from_secs(10),
            CompletionWaker::from_ready(sess.driver(), slot.key()),
        );
        assert!(first.as_ref().is_armed());
        assert!(first.as_ref().cancel());
        assert!(!first.as_ref().cancel());
        timer.flush(sess.driver_access().region_token());

        let second = pin!(Registration::new(timer));
        second.as_ref().arm(
            now + Duration::from_secs(10),
            CompletionWaker::from_ready(sess.driver(), slot.key()),
        );
        assert!(
            second.as_ref().is_armed(),
            "the released fixed slot must be reusable"
        );
        assert!(second.as_ref().cancel());
    });
}

#[test]
fn batch_timer_completions_wake_the_exact_children() {
    with_session_timer_slots(2, |mut sess| {
        let root = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let timer = sess.driver_access().timer();
        let mut batch = pin!(Batch::from_array([
            timer.sleep(Duration::from_millis(10)),
            timer.sleep(Duration::from_millis(10)),
        ]));

        assert!(poll_with_slot(&mut sess, &root, batch.as_mut()).is_pending());
        std::thread::sleep(Duration::from_millis(15));
        timer.expire(sess.driver_access().region_token(), Instant::now());
        assert_eq!(drain_tokens(sess.driver()), [tok(0)]);
        assert!(poll_with_slot(&mut sess, &root, batch.as_mut()).is_ready());
    });
}

#[test]
fn full_timer_does_not_livelock_and_release_wakes_starved() {
    with_session_timer_slots(1, |mut sess| {
        let armed_slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let first_slot = sess.driver().make_ready_slot(tok(1)).expect("ready slot");
        let second_slot = sess.driver().make_ready_slot(tok(2)).expect("ready slot");
        let third_slot = sess.driver().make_ready_slot(tok(3)).expect("ready slot");
        let timer = sess.driver_access().timer();
        let mut held = Box::pin(timer.sleep(Duration::from_secs(100)));
        let mut first = Box::pin(timer.sleep(Duration::from_secs(100)));
        let mut second = Box::pin(timer.sleep(Duration::from_secs(100)));
        let mut third = Box::pin(timer.sleep(Duration::from_secs(100)));
        assert!(poll_with_slot(&mut sess, &armed_slot, held.as_mut()).is_pending());
        for _ in 0..3 {
            assert!(poll_with_slot(&mut sess, &first_slot, first.as_mut()).is_pending());
        }
        assert!(poll_with_slot(&mut sess, &second_slot, second.as_mut()).is_pending());
        assert!(poll_with_slot(&mut sess, &third_slot, third.as_mut()).is_pending());
        assert!(drain_tokens(sess.driver()).is_empty());
        drop(held);
        timer.flush(sess.driver_access().region_token());
        assert_eq!(drain_tokens(sess.driver()), [tok(1)]);
        drop(first);
        assert_eq!(drain_tokens(sess.driver()), [tok(2)]);
        assert!(poll_with_slot(&mut sess, &second_slot, second.as_mut()).is_pending());
        drop(second);
        timer.flush(sess.driver_access().region_token());
        assert_eq!(drain_tokens(sess.driver()), [tok(3)]);
    });
}

#[test]
fn starved_sleep_keeps_its_earlier_deadline() {
    with_session_timer_slots(1, |mut sess| {
        let held_slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let early_slot = sess.driver().make_ready_slot(tok(1)).expect("ready slot");
        let timer = sess.driver_access().timer();
        let mut held = pin!(timer.sleep(Duration::from_secs(100)));
        let mut early = pin!(timer.sleep(Duration::from_millis(20)));
        assert!(poll_with_slot(&mut sess, &held_slot, held.as_mut()).is_pending());
        assert!(poll_with_slot(&mut sess, &early_slot, early.as_mut()).is_pending());
        let earliest = timer
            .earliest(sess.driver_access().region_token_ref())
            .expect("starved deadline");
        assert!(earliest <= Instant::now() + Duration::from_millis(20));
        std::thread::sleep(Duration::from_millis(25));
        timer.expire(sess.driver_access().region_token(), Instant::now());
        assert_eq!(drain_tokens(sess.driver()), [tok(1)]);
        assert!(poll_with_slot(&mut sess, &early_slot, early.as_mut()).is_ready());
    });
}

#[test]
fn far_future_sleep_arms_without_overflow() {
    with_session_timer_slots(1, |mut sess| {
        let timer = sess.driver_access().timer();
        let slot = sess.driver().make_ready_slot(tok(0)).expect("ready slot");
        let mut sleep = pin!(dope_fiber::sleep::Sleep::new(timer, Duration::MAX));
        assert!(
            matches!(
                poll_with_slot(&mut sess, &slot, sleep.as_mut()),
                Poll::Pending
            ),
            "a Duration::MAX deadline must clamp instead of overflowing and stay pending"
        );
    });
}

#[test]
fn starved_queue_survives_rotations_and_arbitrary_cancellation() {
    with_session_timer_slots(0, |mut sess| {
        let timer = sess.driver_access().timer();
        let slots = sess
            .driver()
            .make_ready_slots((0..64u16).map(tok))
            .expect("ready slots");
        let mut sleeps = Vec::new();
        for index in 0..64u32 {
            let slot = slots.get(index as usize).unwrap();
            let mut sleep = Box::pin(timer.sleep(Duration::from_secs(u64::from(64 - index))));
            assert!(poll_with_slot(&mut sess, slot, sleep.as_mut()).is_pending());
            sleeps.push(Some(sleep));
        }
        assert!(
            timer
                .earliest(sess.driver_access().region_token_ref())
                .is_some()
        );
        for step in 0..64usize {
            drop(sleeps[(step * 37) & 63].take());
        }
        assert!(
            timer
                .earliest(sess.driver_access().region_token_ref())
                .is_none()
        );
    });
}
