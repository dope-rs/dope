use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_fiber::{reply::LaneIndex, task::done::Once};
use dope_test::{alloc, harness::scenario};

use crate::common::runtime::{self, Empty, Setup};
pub mod common;

use std::convert::Infallible;

use dope::runtime::executor::drive;
use dope_core::io::Reclaimed;
use dope_fiber::reply::{recv, recv::attach::Attach as _};
use dope_test::alloc::Tracker;

use crate::common::storage::replies::support::{
    Counted, Drain, Drops, Fixture, Maintenance, Never, Releases, RetainOne,
};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement failed: {:?}", failure.cause())
}

#[test]
fn wakes_each_waiter() {
    use std::{pin::pin, task::Poll};

    use dope::core::driver::{
        route::{Epoch, KeyTag, indices::SlotIndex},
        schedule,
    };
    use dope_fiber::{
        context::Context,
        reply::{Arena, Work, layout, usage},
    };

    use crate::common::{
        runtime::profile,
        schedule::tasks::{fixtures::Rounds as _, retained::Retain as _},
    };

    let drops = Drops::new();
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let routes = [0, 1, 2].map(|index| {
                    core.targets::<KeyTag<0>>()
                        .bind(SlotIndex::from(index), Epoch::INITIAL)
                        .dispatch()
                });
                let roots = routes.map(|route| core.ready().make_slot(route).expect("ready slot"));
                let mut driver = context.retained();
                let layout = layout::Layout::new(
                    layout::Capacity::new(
                        layout::Lanes::new(3),
                        layout::Entries::new(3),
                        layout::Data::new(6),
                        usage::Bytes::new(600),
                        usage::Values::new(6),
                    ),
                    layout::Limit::new(
                        layout::Data::new(1),
                        usage::Bytes::new(400),
                        usage::Values::new(1),
                    ),
                )
                .expect("reply layout");
                Arena::try_with(layout, driver.region_token().proof(), |arena| {
                    let lanes = [
                        LaneIndex::ZERO,
                        LaneIndex::new::<1>(),
                        LaneIndex::new::<2>(),
                    ]
                    .map(|index| arena.try_lane(index).expect("reply lane"));
                    let mut source = recv::Response::<_, RetainOne>::new();
                    source
                        .try_attach(driver.region_token(), lanes[0])
                        .expect("source");
                    let value = Counted {
                        failure: None,
                        bytes: 400,
                        drops: &drops,
                    };
                    assert!(
                        lanes[0]
                            .select(driver.region_token())
                            .try_push(value)
                            .is_ok()
                    );
                    lanes[0].complete(driver.region_token());
                    let retained = {
                        let turn = control.begin();
                        let mut source = pin!(dope_fiber::abi::Task::new(source));
                        let result = Context::with_target(
                            roots[0].target(),
                            turn.turn().app(),
                            driver.reborrow(),
                            |context| context.poll(source.as_mut()),
                        );
                        let result = match result {
                            Poll::Ready(result) => Some(result),
                            Poll::Pending => None,
                        }
                        .expect("source must be ready");
                        turn.ready_round(|_| {});
                        result.expect("retained reply")
                    };
                    let held = {
                        let retained = retained.into_parts();
                        retained.1
                    };
                    let mut replies = [
                        recv::Response::<_, Never>::new(),
                        recv::Response::<_, Never>::new(),
                    ];
                    let values;
                    let measure = {
                        let (bytes, retained) = held.into_parts();
                        values = retained;
                        assert!(bytes.is_some(), "retained reply owns byte credit");
                        for (index, receiver) in replies.iter_mut().enumerate() {
                            let lane = lanes[index + 1];
                            lane.bind_retire(roots[index + 1].target());
                            receiver
                                .try_attach(driver.region_token(), lane)
                                .expect("waiter");
                            let value = Counted {
                                failure: None,
                                bytes: 200,
                                drops: &drops,
                            };
                            assert!(matches!(
                                lane.select(driver.region_token()).try_push(value),
                                Err(usage::InsertError::Backpressure { .. })
                            ));
                        }
                        control.begin().ready_round(|_| {});
                        Tracker::begin()
                    };
                    let allocs = measure.finish();
                    assert_eq!((allocs.allocations, allocs.allocated_bytes), (0, 0));
                    assert!(values.is_some(), "retained reply keeps its value credit");
                    assert!(matches!(arena.work(), Work::Pending));
                    assert_eq!(control.begin().ready_round(|_| {}), 0);
                    let mut done = [false; 2];
                    for _ in 0..3 {
                        let turn = control.begin();
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            let work = share.work();
                            assert_eq!(work.remaining(), 1);
                            for _ in 0..3 {
                                arena.maintain(work, driver.region_token());
                            }
                            assert_eq!(work.remaining(), 0);
                        });
                        turn.ready_round(|event| {
                            let index = routes
                                .iter()
                                .position(|route| route.matches(event))
                                .expect("waiter event");
                            assert!(index > 0 && !done[index - 1]);
                            let value = Counted {
                                failure: None,
                                bytes: 200,
                                drops: &drops,
                            };
                            assert!(
                                lanes[index]
                                    .select(driver.region_token())
                                    .try_push(value)
                                    .is_ok()
                            );
                            done[index - 1] = true;
                        });
                        if done == [true; 2] {
                            break;
                        }
                    }
                    assert_eq!(done, [true; 2]);
                })
                .expect("reply arena");
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn cancels_round_frontier() {
    use std::array;

    use dope::core::driver::{
        route::{Epoch, KeyTag, indices::SlotIndex},
        schedule,
    };
    use dope_fiber::reply::{Arena, Work, layout};

    use crate::common::{
        runtime::profile,
        schedule::tasks::{fixtures::Rounds as _, retained::Retain as _},
        storage::replies::support::{capacity, limit},
    };

    let drops = Drops::new();
    Setup::for_profile::<profile::Tasks>().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let routes = [2, 3].map(|index| {
                    core.targets::<KeyTag<0>>()
                        .bind(SlotIndex::from(index), Epoch::INITIAL)
                        .dispatch()
                });
                let roots = routes.map(|route| core.ready().make_slot(route).expect("ready slot"));
                let mut driver = context.retained();
                let layout = layout::Layout::new(capacity(4, 4, 8, 800, 8), limit(1, 500, 1))
                    .expect("reply layout");
                Arena::try_with(layout, driver.region_token().proof(), |arena| {
                    let lanes = [
                        LaneIndex::ZERO,
                        LaneIndex::new::<1>(),
                        LaneIndex::new::<2>(),
                        LaneIndex::new::<3>(),
                    ]
                    .map(|index| arena.try_lane(index).expect("reply lane"));
                    let mut replies: [_; 4] = array::from_fn(|_| recv::Response::<_, Never>::new());
                    for (lane, reply) in lanes.iter().zip(&mut replies) {
                        reply
                            .try_attach(driver.region_token(), *lane)
                            .expect("reply attachment");
                    }
                    for (lane, root) in lanes[2..].iter().zip(&roots) {
                        lane.bind_retire(root.target());
                    }
                    for (index, bytes) in [400, 200, 300, 300].into_iter().enumerate() {
                        let result = lanes[index]
                            .select(driver.region_token())
                            .try_push(Counted {
                                failure: None,
                                bytes,
                                drops: &drops,
                            });
                        assert_eq!(result.is_ok(), index < 2);
                    }
                    control.begin().ready_round(|_| {});

                    replies[1].detach();
                    lanes[1].complete(driver.region_token());
                    for _ in 0..2 {
                        let turn = control.begin();
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            arena.maintain(share.work(), driver.region_token());
                        });
                        assert_eq!(turn.ready_round(|_| {}), 0);
                    }
                    lanes[3].control().deactivate(driver.region_token());
                    for _ in 0..8 {
                        let turn = control.begin();
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            arena.maintain(share.work(), driver.region_token());
                        });
                        assert_eq!(turn.ready_round(|_| {}), 0);
                    }
                    assert!(matches!(arena.work(), Work::Idle));

                    replies[0].detach();
                    lanes[0].complete(driver.region_token());
                    let mut woken = [0; 2];
                    for _ in 0..8 {
                        let turn = control.begin();
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            arena.maintain(share.work(), driver.region_token());
                        });
                        turn.ready_round(|event| {
                            for (route, count) in routes.iter().zip(&mut woken) {
                                if route.matches(event) {
                                    *count += 1;
                                }
                            }
                        });
                    }
                    assert_eq!(woken, [1, 0]);
                    assert!(matches!(arena.work(), Work::Idle));
                })
                .expect("reply arena");
            });
        },
        recovery::Policy::new(|_| {}, recover),
        check_scope,
    );
}

#[test]
fn keeps_release_order() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Lanes(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let left = (arena.try_lane(LaneIndex::ZERO)).expect("left lane exists");
                    let right = (arena.try_lane(LaneIndex::new::<1>())).expect("right lane exists");
                    let mut replies = [
                        recv::Response::<_, RetainOne>::new(),
                        recv::Response::<_, RetainOne>::new(),
                    ];
                    for (response, lane) in replies.iter_mut().zip([left, right]) {
                        let mut driver = session.driver();
                        let region = driver.region_token();
                        assert!((response.try_attach(region, lane)) == (Ok(())));
                        assert!(lane.select(region).try_push(Counted::data(&drops)).is_ok());
                        lane.complete(region);
                    }
                    let [left, right] = replies;
                    let releases = Releases::new();
                    let measure = {
                        let credit;
                        let measure = {
                            let owners = session.with_quiesce(
                                &mut finish,
                                Empty {},
                                scenario::TIMEOUT,
                                |mut app| {
                                    let left = app.drive(Once::new(left))?;
                                    let right = app.drive(Once::new(right))?;
                                    Ok::<_, drive::Error>((left, right))
                                },
                            );
                            assert!(
                                owners.is_ok(),
                                "finalize both replies: {:?}",
                                owners.as_ref().err().map(|error| error.cause())
                            );
                            let owners = owners.ok().expect("finalize both replies");
                            let owners = owners.expect("both replies retain their payloads");
                            let left = owners.0.expect("left reply retention");
                            assert!(owners.1.is_ok(), "right reply retention");
                            let result = session.with_quiesce(
                                &mut finish,
                                Maintenance::new(state, &releases),
                                scenario::TIMEOUT,
                                |mut app| app.drive(Once::new(Drain::new(state))),
                            );
                            assert!(
                                matches!(result, Ok(Ok(()))),
                                "retire while owners remain live"
                            );
                            assert_eq!(arena.inflight().get(), 2);
                            assert!(releases.take().is_empty());
                            let left = left.into_parts();
                            let (bytes, values) = left.1.into_parts();
                            assert!(bytes.is_none());
                            credit = values;
                            Tracker::begin()
                        };
                        assert_eq!(measure.finish().allocations, 0);
                        assert!(credit.is_some(), "the left payload owns value credit");
                        let result = session.with_quiesce(
                            &mut finish,
                            Maintenance::new(state, &releases),
                            scenario::TIMEOUT,
                            |mut app| app.drive(Once::new(Drain::new(state))),
                        );
                        assert!(matches!(result, Ok(Ok(()))), "release the right lane first");
                        let released = releases.take();
                        assert!(released.into_iter().map(|lane| lane.index().get()).eq([1]));
                        assert_eq!(
                            arena.inflight().get(),
                            1,
                            "the left value credit still retains its entry"
                        );

                        Tracker::begin()
                    };
                    assert_eq!(measure.finish().allocations, 0);

                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(matches!(result, Ok(Ok(()))), "drain deferred releases");
                    let released = releases.take();
                    assert!(
                        released.into_iter().map(|lane| lane.index().get()).eq([0]),
                        "partial owners must not duplicate a queued release"
                    );
                    assert_eq!(arena.inflight().get(), 0);
                })
                .expect("reply storage construction");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(drops.count(), 2);
}
