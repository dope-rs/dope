use std::{cell::Cell, num::NonZero, pin, rc::Rc};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{self, app::step::Flow, ready::task::owner},
};
use dope_fiber::{
    context::wake::RootWaker,
    task::storage::{
        fixed::{
            local::Removal,
            schedule::{Pool, queue::Owned, result::Selection},
        },
        scope::Scope,
    },
};
use dope_test::alloc::Tracker;
use o3::depot::capacity::Count;

pub mod common;
use common::{
    recover,
    runtime::{self, Setup, profile},
    schedule::tasks::fixtures::{Rounds as _, WakeProbe},
    storage::policies::owner::{Stamp, Tasks},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn transfer_rearms_parent() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_scope(
        |scope| {
            let (_, context, mut control) = scope.split();
            let view = context.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let routes = [0, 1].map(|index| {
                targets
                    .bind(SlotIndex::from(index), Epoch::INITIAL)
                    .dispatch()
            });
            let roots = routes.map(|route| view.ready().make_slot(route).expect("parent slot"));
            let owners =
                [0, 1].map(|lane| owner::Owner::from(Stamp::try_new(lane, 0).expect("owner")));
            let plane = Pool::<'_, WakeProbe<'_>, (), usize, Owned<'_, Tasks>>::try_owned(
                Count::new(4),
                owner::Capacity::new(NonZero::new(2).expect("owners")),
            )
            .expect("owner queue");
            let mut plane = pin::pin!(plane);
            let mut driver = context;
            let polls = [0, 1, 2, 3].map(|_| Rc::new(Cell::new(0)));
            let wakes = [0, 1, 2, 3].map(|_| Rc::new(Cell::new(None)));
            Scope::with(plane.as_mut(), |mut plane| {
                for owner in owners {
                    assert!(
                        plane.as_ref().owners().register(owner).is_ok(),
                        "registration"
                    );
                }
                let ids = [0, 1, 2, 3].map(|index| {
                    let owner = usize::from(index != 2);
                    plane
                        .try_admit(owners[owner], RootWaker::from(roots[owner].target()))
                        .ok()
                        .expect("task admission")
                        .insert((
                            WakeProbe {
                                polls: polls[index].clone(),
                                wake: wakes[index].clone(),
                            },
                            index,
                        ))
                });
                {
                    let turn = control.begin();
                    for owner in [1, 1, 0] {
                        let item = match plane
                            .as_mut()
                            .select(owner::Scope::Exact(owners[owner]), turn.turn().app())
                        {
                            Selection::Ready(item) => Some(item),
                            Selection::Idle | Selection::Yielded => None,
                        }
                        .expect("initial task");
                        item.step(&mut driver).1.settle(Flow::Yield);
                    }
                    turn.ready_round(|_| {});
                }
                roots[0].target().wake();
                for wake in &wakes[..3] {
                    wake.take().expect("task wake").wake();
                }
                let alloc = Tracker::during(|| {
                    let mut first = true;
                    for _ in 0..4 {
                        let turn = control.begin();
                        turn.ready_round(|event| {
                            let index = routes
                                .iter()
                                .position(|route| route.matches(event))
                                .expect("parent event");
                            if first {
                                assert_eq!(
                                    index, 0,
                                    "foreign parent dispatch must be consumed first"
                                );
                                first = false;
                            }
                            let owner = owner::Scope::Exact(owners[index]);
                            turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                                if let Selection::Ready(item) =
                                    plane.as_mut().select(owner, share.work())
                                {
                                    item.step(&mut driver).1.settle(Flow::Yield);
                                }
                            });
                            plane.as_mut().wake(owner);
                        });
                    }
                });
                assert_eq!(alloc, (0, 0));
                assert_eq!(
                    polls[2].get(),
                    2,
                    "foreign parent must dispatch after its ingress moved"
                );
                for id in ids {
                    assert!(matches!(plane.remove(id), Removal::Removed));
                }
                for owner in owners {
                    assert!(
                        plane.as_ref().owners().try_retire(owner).is_ok(),
                        "retirement"
                    );
                }
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
