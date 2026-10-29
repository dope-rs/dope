use std::time::Duration;

use dope_core::driver::schedule::clock;
use dope_runtime::civil::{self, Source as _};
use o3::base::time::{Nanos, Sub, Unix};

pub mod common;
use common::civil::Source;

#[test]
fn anchor_projects_elapsed() {
    clock::Scope::run(|scope| {
        let origin = scope.now();
        let raw = origin.into_raw();
        let civil = Source(Unix::<Nanos>::new(13), raw + Duration::from_nanos(3));
        let anchor = (civil.anchor(&origin)).expect("required fixture outcome");
        assert!(anchor.observe(&origin) == Ok(Unix::<Nanos>::new(10)));
        let later = origin.moment_at(raw + Duration::from_nanos(7));
        assert!(anchor.observe(&later) == Ok(Unix::<Nanos>::new(17)));
        let earlier = raw - Duration::from_nanos(1);
        assert!(
            anchor.observe(&origin.moment_at(earlier))
                == Err(civil::Error::MonotonicRegression {
                    origin: raw,
                    observed: earlier,
                })
        );
    });
}

#[test]
fn admits_sample_pair() {
    clock::Scope::run(|scope| {
        let origin = scope.now();
        let raw = origin.into_raw();
        let epoch = Unix::<Nanos>::new(1);
        let elapsed = Duration::from_nanos(2);
        let sample = Source(epoch, raw + elapsed);
        assert!(matches!(
            sample.anchor(&origin),
            Err(civil::Error::Sub { source: Sub::Range { at, duration } })
                if at == epoch && duration == elapsed
        ));
        let earlier = raw - elapsed;
        let sample = Source(epoch, earlier);
        assert!(matches!(
            sample.anchor(&origin),
            Err(civil::Error::MonotonicRegression { origin, observed })
                if origin == raw && observed == earlier
        ));
    });
}
