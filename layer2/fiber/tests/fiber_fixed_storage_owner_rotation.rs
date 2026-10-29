use dope::core::driver::life::recovery;
pub mod common;

use std::{convert::Infallible, num::NonZero, pin};

use common::{
    runtime::{self, Setup, profile},
    storage::policies::owner::{Stamp, Tasks},
};
use dope::core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{
        app::step::Flow,
        ready::task::owner::{Capacity, Owner, Scope},
    },
};
use dope_fiber::{
    abi::Ready,
    context::wake::RootWaker,
    task::storage::{
        fixed::{
            quota::Physical,
            schedule::{Plane, Pool, queue::Owned, result::Selection},
            tree::{Here, Leaf},
        },
        scope,
    },
};

#[test]
fn exact_moves_to_back() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let root = view
                .ready()
                .make_slot(
                    view.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let pool = Pool::<'_, Ready<()>, 4, (), u8>::try_new().expect("task pool");
            let plane = Plane::<'_, _, Physical, Owned<'_, Tasks>>::try_owned(
                Leaf::new(pool),
                Capacity::new(NonZero::new(3).expect("owner count")),
            )
            .expect("owner queue");
            let mut plane = pin::pin!(plane);
            let owners =
                [0, 1, 2].map(|lane| Owner::from(Stamp::try_new(lane, 0).expect("owner identity")));
            scope::Scope::with(plane.as_mut(), |mut plane| {
                for owner in owners {
                    plane
                        .as_ref()
                        .owners()
                        .register(owner)
                        .map_err(|error| error.cause())
                        .expect("owner registration");
                }
                for lane in [0, 1, 2, 1] {
                    let _id = plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit(owners[lane], RootWaker::from(root.target()))
                        .ok()
                        .expect("task admission")((
                        Ready::new(()),
                        lane as u8,
                    ));
                }
                for (scope, target) in [
                    (Scope::Exact(owners[1]), 1),
                    (Scope::Any, 0),
                    (Scope::Any, 2),
                    (Scope::Any, 1),
                ] {
                    let item = match plane.as_mut().select(scope, turn.turn().app()) {
                        Selection::Ready(item) => Some(item),
                        _ => None,
                    }
                    .expect("ready task");
                    let entry = item.only();
                    assert_eq!(entry.target(), target);
                    entry.step(&mut driver).1.settle(Flow::Batch);
                }
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty());
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement failed: {:?}", failure.cause())
}
