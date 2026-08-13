use std::time::Duration;

use dope::core::driver::settings;
use dope_test::scenario::rt::Runtime;

#[test]
fn unrepresentable_sleep_is_rejected() {
    Runtime::throughput()
        .timer_cache_limit(settings::ScheduleCapacity::fixed::<1>())
        .with_session(|mut sess| {
            let timer = sess.driver_access().timer();
            assert!(
                dope_fiber::task::sleep::Sleep::new(timer, Duration::MAX).is_err(),
                "an unrepresentable deadline must be rejected"
            );
        });
}
