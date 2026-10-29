use dope::core::driver::life::recovery;

pub mod common;

use std::{cell::Cell, convert::Infallible, pin::pin, rc::Rc, task::Poll};

use common::{
    runtime::{self, Setup, profile},
    storage::stores::{Domain, DropReady},
};
use dope::core::driver::route::{Epoch, KeyTag, indices::SlotIndex};
use dope_fiber::{
    context::Context,
    task::storage::{
        fixed::{
            direct::Plane,
            quota::Limit,
            staged::{self, Completion as _, State, Store as _},
            tree::{Here, Leaf},
        },
        scope::Scope,
    },
};
use o3::depot::arena::slab::storage;

#[test]
fn removes_once() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let root = core
                .ready()
                .make_slot(
                    core.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let drops = Rc::new(Cell::new(0));
            let slots = staged::Slab::<'_, DropReady, 1, Domain, storage::Heap>::try_new()
                .expect("one heap task slot");
            let mut plane = pin!(Plane::bounded(Leaf::new(slots), Limit::new(1)));
            Scope::with(plane.as_mut(), |mut plane| {
                for phase in [State::Live, State::Staged, State::Vacant] {
                    let mut route = plane.as_mut().route::<Here>();
                    let id = route
                        .insert(DropReady {
                            output_drops: Rc::clone(&drops),
                        })
                        .expect("task slot");
                    if phase != State::Live {
                        let poll = Context::with_target(
                            root.target(),
                            turn.turn().app(),
                            driver.reborrow(),
                            |mut context| route.as_mut().poll_staged(&id, context.as_mut()),
                        );
                        assert!(poll == Some(staged::Poll::NewlyReady));
                    }
                    if phase == State::Vacant {
                        drop(route.as_mut().take_output(&id).expect("retained output"));
                    }
                    assert!(route.as_mut().remove_state(id) == Some(phase));
                    assert_eq!(plane.available().get(), 1);
                }
                assert_eq!(drops.get(), 2);
                let mut route = plane.as_mut().route::<Here>();
                let id = route
                    .insert(DropReady {
                        output_drops: Rc::clone(&drops),
                    })
                    .expect("task slot");
                Context::with_target(
                    root.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |mut context| {
                        let ready = match route.as_mut().poll_ready(&id, context.as_mut()) {
                            Some(Poll::Ready((ready, _))) => Some(ready),
                            _ => None,
                        }
                        .expect("ready task");
                        drop(ready.finish());
                    },
                );
                assert!(route.as_mut().remove_state(id).is_none());
                assert_eq!(plane.available().get(), 1);
                assert_eq!(drops.get(), 3);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty());
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
