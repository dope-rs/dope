use std::time::Duration;

use dope_core::driver::schedule::clock;
use o3::base::time::Positive;

#[test]
fn positive_overflow() {
    clock::Scope::run(|scope| {
        let now = scope.now();
        let duration = Positive::from_secs::<{ u64::MAX }>();
        let result = now.try_add(duration);
        assert!(result.is_err());
        if let Err(range) = result {
            let retained: Positive = range.duration();
            assert!(retained == duration);
        }
    });
}

#[test]
fn duration_overflow() {
    clock::Scope::run(|scope| {
        let now = scope.now();
        let duration = Duration::MAX;
        let result = now.try_add(duration);
        assert!(result.is_err());
        if let Err(range) = result {
            assert_eq!(range.duration(), duration);
        }
        let last = now.clamp_add(duration);
        assert!(last > now);
        let step = Duration::from_nanos(1);
        assert!(last.into_raw().checked_add(step).is_none());
        assert_eq!(last.clamp_add(step), last);
        let offset = last.try_left(now).expect("future deadline");
        assert_eq!(now.try_add(offset), Ok(last));
        assert!(now.try_add(Duration::ZERO).is_ok_and(|due| due == now));
    });
}
