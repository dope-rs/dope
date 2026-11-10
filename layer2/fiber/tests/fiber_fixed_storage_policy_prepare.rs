use dope_fiber::{
    abi::Ready,
    task::storage::fixed::{
        local::Removal,
        schedule::queue::{Error, OwnerError},
    },
};
use dope_test::{alloc, alloc::Tracker, scope};

pub mod common;
use crate::common::storage::policies::prepare::{self, with_plane};
use common::recover;

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn keeps_vacancy() {
    let reports = with_plane(
        |mut plane, parent, owner| {
            let alloc = Tracker::during(|| {
                let rejected = plane
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

                let task = plane.try_admit(owner, parent);
                let task = task
                    .ok()
                    .expect("preserved vacancy")
                    .insert((Ready::new(()), 2));
                assert!((plane.remove(task)) == (Removal::Removed));
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
fn abandoned_admission() {
    let reports = with_plane(
        |mut plane, parent, owner| {
            assert!(
                plane.as_ref().owners().register(owner).is_ok(),
                "registration"
            );
            let alloc = Tracker::during(|| {
                let admit = plane.try_admit(owner, parent);
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
                let task = plane.try_admit(owner, parent);
                let task = task
                    .ok()
                    .expect("abandoned reservation preserves the slot")
                    .insert((Ready::new(()), 3));
                assert!((plane.remove(task)) == (Removal::Removed));
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
