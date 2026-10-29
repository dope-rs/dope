use std::convert::Infallible;

use dope::core::driver::life::recovery;
use dope_fiber::{
    abi::Ready,
    task::storage::fixed::{
        local::Removal,
        schedule::queue::{Error, OwnerError, RetireError},
        tree::Here,
    },
};
use dope_test::{alloc, alloc::Tracker, scope};

pub mod common;
use crate::common::storage::policies::prepare::{self, with_plane};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn keeps_vacancy() {
    let reports = with_plane(
        |mut plane, parent, owner| {
            let alloc = Tracker::during(|| {
                let rejected = plane
                    .as_mut()
                    .route::<Here>()
                    .try_admit(owner, parent)
                    .err()
                    .expect("unregistered owner must fail admission");
                assert!(matches!(
                    rejected,
                    Error::Owner(OwnerError::Stale {
                        owner: actual,
                        current: None,
                    }) if actual == owner
                ));
                assert!(
                    plane.as_ref().owners().register(owner).is_ok(),
                    "registration"
                );
                let mut route = plane.as_mut().route::<Here>();
                let task = route.try_admit(owner, parent);
                let task = task.ok().expect("preserved vacancy")((Ready::new(()), 2));
                assert!((plane.as_mut().route::<Here>().remove(task)) == (Removal::Removed));
                assert!(
                    plane.as_ref().owners().try_retire(owner).is_ok(),
                    "retirement"
                );
            });

            assert_eq!(alloc, (0, 0));
        },
        recover,
        check_plane,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn keeps_bound() {
    let reports = with_plane(
        |mut plane, parent, owner| {
            assert!(
                plane.as_ref().owners().register(owner).is_ok(),
                "registration"
            );
            let mut route = plane.as_mut().route::<Here>();
            let task = route.try_admit(owner, parent);
            let task = task.ok().expect("registered owner admission")((Ready::new(()), 1));

            assert!(matches!(
                plane.as_ref().owners().try_retire(owner),
                Err(RetireError::Busy {
                    owner: actual,
                    tasks,
                }) if actual == owner && tasks.nonzero().get() == 1
            ));
            assert!((plane.as_mut().route::<Here>().remove(task)) == (Removal::Removed));
            assert!(
                plane.as_ref().owners().try_retire(owner).is_ok(),
                "retirement"
            );
        },
        recover,
        check_plane,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn abandoned_admission() {
    let reports = with_plane(
        |mut plane, parent, owner| {
            assert!(
                plane.as_ref().owners().register(owner).is_ok(),
                "registration"
            );
            let alloc = Tracker::during(|| {
                let mut route = plane.as_mut().route::<Here>();
                let admit = route.try_admit(owner, parent);
                assert!(admit.is_ok(), "reserve one task");
            });
            assert_eq!(alloc, (0, 0));
            assert!(
                plane.as_ref().owners().try_retire(owner).is_ok(),
                "retirement"
            );
            assert!(
                plane.as_ref().owners().register(owner).is_ok(),
                "registration"
            );
            let alloc = Tracker::during(|| {
                let mut route = plane.as_mut().route::<Here>();
                let task = route.try_admit(owner, parent);
                let task = task.ok().expect("abandoned reservation preserves the slot")((
                    Ready::new(()),
                    3,
                ));
                assert!((plane.as_mut().route::<Here>().remove(task)) == (Removal::Removed));
            });
            assert_eq!(alloc, (0, 0));
            assert!(
                plane.as_ref().owners().try_retire(owner).is_ok(),
                "retirement"
            );
        },
        recover,
        check_plane,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn check_plane(
    result: Result<Result<(), prepare::Error>, scope::Error<'_, Result<(), prepare::Error>>>,
) {
    result
        .as_ref()
        .map_err(scope::Error::cause)
        .expect("driver lifecycle")
        .as_ref()
        .expect("owned plane fixture");
}
