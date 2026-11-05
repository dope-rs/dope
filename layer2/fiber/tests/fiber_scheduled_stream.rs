use std::{num::NonZero, pin};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{
        self,
        app::step::Flow,
        ready::task::owner::{Capacity, Owner, Scope::Exact},
    },
};
use dope_fiber::{
    context::wake::RootWaker,
    task::storage::{
        fixed::{
            local::Removal,
            quota::{Limit, Physical},
            schedule::{
                Plane, Stream,
                queue::{Owned, RetireError},
                result::{Outcome, Resume, Selection, Update},
            },
            tree::{Here, Leaf},
        },
        scope::Scope,
    },
};
use dope_test::alloc::Tracker;

pub mod common;
use common::{
    recover,
    runtime::{self, Setup, profile},
    storage::{
        policies::{Domain, owner},
        streams::{Failure, Producer, State},
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn holds_items_until_resumed() {
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
            let parent = RootWaker::from(root.target());
            let state = State::default();
            let replacement = State::default();
            let pool =
                Stream::<'_, Producer<'_, '_>, 1, Domain, u8>::try_new().expect("stream storage");
            let mut plane = pin::pin!(Plane::bounded(Leaf::new(pool), Limit::new(1)));
            let input = Box::new([9, 3]);
            let address = input.as_ptr();
            Scope::with(plane.as_mut(), |mut plane| {
                let allocations = Tracker::during(|| {
                    let id = plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("stream admission")((
                        Producer::new(&state, true, 7),
                        41,
                    ));
                    let input =
                        match plane
                            .as_mut()
                            .route::<Here>()
                            .try_update(&id, input, |_, _| panic!("active stream update"))
                        {
                            Err(Update::Active(input)) => input,
                            _ => panic!("active update refusal"),
                        };
                    assert_eq!(input.as_ptr(), address);
                    assert_eq!(plane.available().get(), 0);

                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        let Selection::Ready(selected) = plane.as_mut().select(share.work()) else {
                            panic!("initial stream work");
                        };
                        assert!(matches!(
                            selected.only().drive(&mut driver),
                            Outcome::Pending { target: 41 }
                        ));
                        assert_eq!(share.work().remaining(), 0);
                    });
                    assert_eq!(state.polls.get(), 1);
                    assert!(matches!(
                        plane.as_mut().select(turn.turn().app()),
                        Selection::Idle
                    ));
                    state.wake.get().expect("pending stream wake").wake();
                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        let Selection::Ready(selected) = plane.as_mut().select(share.work()) else {
                            panic!("woken stream work");
                        };
                        assert!(matches!(
                            selected.only().drive(&mut driver),
                            Outcome::Yielded {
                                target: 41,
                                output: Ok(7),
                            }
                        ));
                        assert_eq!(share.work().remaining(), 0);
                    });
                    assert_eq!(state.polls.get(), 2);
                    assert_eq!(state.drops.get(), 0);
                    assert_eq!(plane.available().get(), 0);
                    assert!(
                        plane
                            .as_mut()
                            .route::<Here>()
                            .try_admit((), parent)
                            .is_err()
                    );

                    state.wake.get().expect("held stream wake").wake();
                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        match plane.as_mut().select(share.work()) {
                            Selection::Ready(selected) => assert!(matches!(
                                selected.only().drive(&mut driver),
                                Outcome::Pending { target: 41 }
                            )),
                            Selection::Idle | Selection::Yielded => {}
                        }
                        assert_eq!(share.work().remaining(), 0);
                    });
                    assert_eq!(state.polls.get(), 2, "held wake cannot poll the producer");
                    assert!(matches!(
                        plane.as_mut().select(turn.turn().app()),
                        Selection::Idle
                    ));
                    let input = plane
                        .as_mut()
                        .route::<Here>()
                        .try_update(&id, input, |producer, input| {
                            producer.update(Err(Failure(input[0])));
                            input
                        })
                        .ok()
                        .expect("held stream update");
                    assert_eq!(input.as_ptr(), address);
                    assert!(matches!(
                        plane.as_mut().select(turn.turn().app()),
                        Selection::Idle
                    ));
                    assert!(matches!(
                        plane.as_mut().route::<Here>().resume(&id),
                        Resume::Resumed
                    ));
                    assert!(matches!(
                        plane.as_mut().route::<Here>().resume(&id),
                        Resume::Active
                    ));
                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        let Selection::Ready(selected) = plane.as_mut().select(share.work()) else {
                            panic!("resumed stream work");
                        };
                        assert!(matches!(
                            selected.only().drive(&mut driver),
                            Outcome::Yielded {
                                target: 41,
                                output: Err(Failure(9)),
                            }
                        ));
                        assert_eq!(share.work().remaining(), 0);
                    });
                    assert_eq!(state.polls.get(), 3);
                    assert_eq!(plane.available().get(), 0, "error item retains admission");
                    assert!(matches!(
                        plane.as_mut().route::<Here>().resume(&id),
                        Resume::Resumed
                    ));
                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        let Selection::Ready(selected) = plane.as_mut().select(share.work()) else {
                            panic!("stream EOF work");
                        };
                        assert!(matches!(
                            selected.only().drive(&mut driver),
                            Outcome::Complete {
                                target: 41,
                                output: ()
                            }
                        ));
                        assert_eq!(share.work().remaining(), 0);
                    });
                    assert_eq!(state.polls.get(), 4);
                    assert_eq!(state.drops.get(), 1);
                    assert_eq!(plane.available().get(), 1);

                    let current = plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("reused stream slot")((
                        Producer::new(&replacement, false, 8),
                        42,
                    ));
                    assert!(matches!(
                        plane.as_mut().route::<Here>().resume(&id),
                        Resume::Absent
                    ));
                    let input =
                        match plane
                            .as_mut()
                            .route::<Here>()
                            .try_update(&id, input, |_, _| panic!("stale stream update"))
                        {
                            Err(Update::Absent(input)) => input,
                            _ => panic!("stale update refusal"),
                        };
                    assert_eq!(input.as_ptr(), address);
                    assert_eq!(*input, [9, 3]);
                    assert!(matches!(
                        plane.as_mut().route::<Here>().remove(id),
                        Removal::Absent
                    ));
                    assert!(matches!(
                        plane.as_mut().route::<Here>().remove(current),
                        Removal::Removed
                    ));
                    assert_eq!(replacement.polls.get(), 0);
                    assert_eq!(replacement.drops.get(), 1);
                    assert_eq!(plane.available().get(), 1);
                });
                assert_eq!(allocations, (0, 0));
            });
            assert_eq!(state.drops.get(), 1);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn held_stream_keeps_its_owner() {
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
            let parent = RootWaker::from(root.target());
            let states = [State::default(), State::default()];
            let pool =
                Stream::<'_, Producer<'_, '_>, 2, Domain, u8>::try_new().expect("stream storage");
            let plane = Plane::<'_, _, Physical, Owned<'_, owner::Tasks>>::try_owned(
                Leaf::new(pool),
                Capacity::new(NonZero::new(2).expect("owner capacity")),
            )
            .expect("owner storage");
            let mut plane = pin::pin!(plane);
            let owners = [0, 1]
                .map(|lane| Owner::from(owner::Stamp::try_new(lane, 0).expect("owner identity")));
            Scope::with(plane.as_mut(), |mut plane| {
                let allocations = Tracker::during(|| {
                    let [first, second] = [0, 1].map(|index| {
                        assert!(plane.as_ref().owners().register(owners[index]).is_ok());
                        plane
                            .as_mut()
                            .route::<Here>()
                            .try_admit(owners[index], parent)
                            .ok()
                            .expect("owned stream admission")((
                            Producer::new(&states[index], false, index as u8),
                            index as u8,
                        ))
                    });
                    for index in [0, 1] {
                        let Selection::Ready(selected) = plane
                            .as_mut()
                            .select(Exact(owners[index]), turn.turn().app())
                        else {
                            panic!("owned stream work");
                        };
                        let (outcome, receipt) = selected.only().step(&mut driver);
                        assert!(matches!(outcome,
                            Outcome::Yielded { target, output: Ok(value) }
                                if target == index as u8 && value == target));
                        assert!(receipt.settle(Flow::Batch) == Flow::Batch);
                        assert!(matches!(
                            plane.as_ref().owners().try_retire(owners[index]),
                            Err(RetireError::Busy { owner, tasks })
                                if owner == owners[index] && tasks.nonzero().get() == 1
                        ));
                    }
                    assert_eq!(
                        states[0].polls.get(),
                        1,
                        "other owner made progress while held"
                    );
                    assert!(matches!(
                        plane.as_mut().route::<Here>().resume(&first),
                        Resume::Resumed
                    ));
                    assert!(matches!(
                        plane.as_mut().select(Exact(owners[1]), turn.turn().app()),
                        Selection::Idle
                    ));
                    let Selection::Ready(selected) =
                        plane.as_mut().select(Exact(owners[0]), turn.turn().app())
                    else {
                        panic!("original owner resumed");
                    };
                    let (outcome, receipt) = selected.only().step(&mut driver);
                    assert!(matches!(
                        outcome,
                        Outcome::Complete {
                            target: 0,
                            output: ()
                        }
                    ));
                    assert!(receipt.settle(Flow::Batch) == Flow::Batch);
                    assert_eq!(states[0].drops.get(), 1);
                    assert!(plane.as_ref().owners().try_retire(owners[0]).is_ok());
                    assert!(matches!(
                        plane.as_mut().route::<Here>().remove(second),
                        Removal::Removed
                    ));
                    assert_eq!(states[1].polls.get(), 1);
                    assert_eq!(states[1].drops.get(), 1);
                    assert!(plane.as_ref().owners().try_retire(owners[1]).is_ok());
                    assert!(matches!(
                        plane.as_mut().route::<Here>().remove(first),
                        Removal::Absent
                    ));
                });
                assert_eq!(allocations, (0, 0));
            });
            assert_eq!([states[0].drops.get(), states[1].drops.get()], [1, 1]);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}
