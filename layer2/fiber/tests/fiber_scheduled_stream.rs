use o3::depot::capacity::Count;
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
            schedule::{
                Storage, Stream,
                queue::{Owned, RetireError},
                result::{Error, Outcome, Selection},
            },
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
        streams::{Failure, Mixed, MixedProjection, Mode, Producer, State},
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
            let pool = Storage::<'_, Mixed<'_, '_>, Domain, u8, Mode>::try_new(Count::new(1))
                .expect("mixed storage");
            let mut plane = pin::pin!(pool);
            Scope::with(plane.as_mut(), |mut plane| {
                let allocations = Tracker::during(|| {
                    for cancel in [true, false] {
                        let fixed = plane
                            .try_admit((), parent)
                            .expect("fixed admission")
                            .insert((Mixed::Fixed(dope_fiber::abi::Ready::new(())), 0));

                        let mut entry = plane.entry(fixed).ok().expect("fixed member");
                        assert!(matches!(entry.get(), Mixed::Fixed(_)));
                        if cancel {
                            entry.remove();
                            assert!(plane.try_admit((), parent).is_ok());
                            assert!(matches!(
                                plane.as_mut().select(turn.turn().app()),
                                Selection::Idle
                            ));
                            continue;
                        }
                        let fixed = entry.into_id();
                        assert!(matches!(plane.get_mut(&fixed), Err(Error::Absent)));
                        assert!(plane.try_admit((), parent).is_err());
                        let Selection::Ready(selected) = plane.as_mut().select(turn.turn().app())
                        else {
                            panic!("fixed work");
                        };
                        assert!(matches!(
                            selected.drive(&mut driver),
                            Outcome::Complete {
                                target: 0,
                                output: Some(())
                            }
                        ));
                    }
                    let id = plane
                        .try_admit((), parent)
                        .expect("stream admission")
                        .insert((
                            Mixed::Stream {
                                producer: Producer::new(&state, true, 7),
                                held: false,
                            },
                            41,
                        ));
                    assert!(matches!(plane.get_mut(&id), Err(Error::Active)));
                    let Selection::Ready(selected) = plane.as_mut().select(turn.turn().app())
                    else {
                        panic!("initial stream work");
                    };
                    assert!(matches!(
                        selected.drive(&mut driver),
                        Outcome::Pending { target: 41 }
                    ));
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
                            selected.drive(&mut driver),
                            Outcome::Yielded {
                                target: 41,
                                output: Ok(7),
                            }
                        ));
                        assert_eq!(share.work().remaining(), 0);
                    });
                    assert_eq!(state.polls.get(), 2);
                    assert_eq!(state.drops.get(), 0);
                    assert!(plane.try_admit((), parent).is_err());

                    state.wake.get().expect("held stream wake").wake();
                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        match plane.as_mut().select(share.work()) {
                            Selection::Ready(selected) => assert!(matches!(
                                selected.drive(&mut driver),
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

                    let MixedProjection::Stream { producer, .. } =
                        plane.get_mut(&id).expect("held stream").project()
                    else {
                        panic!("stream phase");
                    };
                    producer.update(Err(Failure(9)));
                    assert!(matches!(
                        plane.as_mut().select(turn.turn().app()),
                        Selection::Idle
                    ));
                    assert!(matches!(plane.resume(&id), Ok(())));
                    assert!(matches!(plane.resume(&id), Err(Error::Active)));
                    let Selection::Ready(selected) = plane.as_mut().select(turn.turn().app())
                    else {
                        panic!("resumed stream work");
                    };
                    assert!(matches!(
                        selected.drive(&mut driver),
                        Outcome::Yielded {
                            target: 41,
                            output: Err(Failure(9)),
                        }
                    ));
                    assert_eq!(state.polls.get(), 3);
                    assert!(plane.try_admit((), parent).is_err());
                    assert!(matches!(plane.resume(&id), Ok(())));
                    let Selection::Ready(selected) = plane.as_mut().select(turn.turn().app())
                    else {
                        panic!("stream EOF work");
                    };
                    assert!(matches!(
                        selected.drive(&mut driver),
                        Outcome::Complete {
                            target: 41,
                            output: None
                        }
                    ));
                    assert_eq!(state.polls.get(), 4);
                    assert_eq!(state.drops.get(), 1);
                    assert!(plane.try_admit((), parent).is_ok());

                    let current = plane
                        .try_admit((), parent)
                        .expect("reused stream slot")
                        .insert((
                            Mixed::Stream {
                                producer: Producer::new(&replacement, false, 8),
                                held: false,
                            },
                            42,
                        ));
                    assert!(matches!(plane.resume(&id), Err(Error::Absent)));
                    assert!(matches!(plane.get_mut(&id), Err(Error::Absent)));
                    assert!(matches!(plane.remove(current), Removal::Removed));
                    assert_eq!(replacement.polls.get(), 0);
                    assert_eq!(replacement.drops.get(), 1);
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
            let plane =
                Stream::<'_, Producer<'_, '_>, Domain, u8, Owned<'_, owner::Tasks>>::try_owned(
                    Count::new(2),
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
                            .try_admit(owners[index], parent)
                            .ok()
                            .expect("owned stream admission")
                            .insert((
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
                        let (outcome, receipt) = selected.step(&mut driver);
                        assert!(matches!(outcome,
                            Outcome::Yielded { target, output: Ok(value) }
                                if target == index as u8 && value == target));
                        receipt.settle(Flow::Batch);
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
                    assert!(matches!(plane.resume(&first), Ok(())));
                    assert!(matches!(
                        plane.as_mut().select(Exact(owners[1]), turn.turn().app()),
                        Selection::Idle
                    ));
                    let Selection::Ready(selected) =
                        plane.as_mut().select(Exact(owners[0]), turn.turn().app())
                    else {
                        panic!("original owner resumed");
                    };
                    let (outcome, receipt) = selected.step(&mut driver);
                    assert!(matches!(
                        outcome,
                        Outcome::Complete {
                            target: 0,
                            output: ()
                        }
                    ));
                    receipt.settle(Flow::Batch);
                    assert_eq!(states[0].drops.get(), 1);
                    assert!(plane.as_ref().owners().try_retire(owners[0]).is_ok());
                    assert!(matches!(plane.remove(second), Removal::Removed));
                    assert_eq!(states[1].polls.get(), 1);
                    assert_eq!(states[1].drops.get(), 1);
                    assert!(plane.as_ref().owners().try_retire(owners[1]).is_ok());
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
