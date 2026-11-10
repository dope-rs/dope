use dope_fiber::task::storage::{fixed::local::Removal, scope::Scope};

use crate::common::runtime::profile;
pub mod common;
use common::recover;

use std::{cell::Cell, ops::ControlFlow, pin::pin, ptr::NonNull, rc::Rc, task};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule,
};
use dope_fiber::{
    context::Context,
    task::storage::fixed::{self, local::Member as _},
};
use dope_test::alloc::Tracker;

use crate::common::{
    runtime::{self, Setup},
    storage::{
        stores::*,
        streams::{Producer, State},
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn updates_pinned_stream() {
    let observed = Rc::new(Cell::new((0, true)));
    let updated = Rc::new(Cell::new((0, true)));
    let mut stores = pin!(fixed::Slab::<'_, Pinned, 1, Domain>::new());
    let allocations = Tracker::during(|| {
        Scope::with(stores.as_mut(), |mut stores| {
            let task = stores
                .insert(Pinned {
                    address: Cell::new(None),
                    observed: observed.clone(),
                })
                .expect("stream slot");
            let task = {
                let (task, mut member) = stores.entry(task).ok().expect("live stream").into_parts();
                let fiber = member.value();
                let address = NonNull::from(fiber.as_ref().get_ref());
                let fields = fiber.project();
                fields.address.set(Some(address));
                *fields.observed = updated.clone();
                task
            };
            assert!(stores.remove(task) == Removal::Removed);
        });
    });
    assert_eq!(allocations, (0, 0));
    assert_eq!(updated.get(), (1, true));
}

#[test]
fn deferred_completion() {
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
            let observed = Rc::new(Cell::new((0, true)));
            let drops = Rc::new(Cell::new(0));
            let mut slab = pin!(fixed::Slab::<'_, Deferred, 1, Domain>::new());
            let activity = Tracker::during(|| {
                Scope::with(slab.as_mut(), |mut slab| {
                    for (complete, take) in [(false, false), (true, false), (true, true)] {
                        observed.set((0, true));
                        drops.set(0);
                        let id = {
                            let id = slab
                                .insert(Deferred::Live(Complete {
                                    pinned: Pinned {
                                        address: Cell::new(None),
                                        observed: observed.clone(),
                                    },
                                    output: Some(DropOutput {
                                        drops: drops.clone(),
                                    }),
                                    pending: true,
                                }))
                                .expect("task slot");
                            let entry = slab.entry(id).ok().expect("live task");
                            let remaining = turn.turn().app().remaining();
                            let ControlFlow::Continue((entry, task::Poll::Pending)) =
                                Context::with_target(
                                    root.target(),
                                    turn.turn().app(),
                                    driver.reborrow(),
                                    |context| entry.poll_next(context),
                                )
                            else {
                                panic!("pending task");
                            };
                            assert_eq!(turn.turn().app().remaining(), remaining - 1);
                            if complete {
                                let ControlFlow::Continue((entry, task::Poll::Ready(()))) =
                                    Context::with_target(
                                        root.target(),
                                        turn.turn().app(),
                                        driver.reborrow(),
                                        |context| entry.poll_next(context),
                                    )
                                else {
                                    panic!("completed task");
                                };
                                assert_eq!(observed.get(), (1, true));
                                assert_eq!(turn.turn().app().remaining(), remaining - 2);
                                let (id, member) = entry.into_parts();
                                drop(member);
                                id
                            } else {
                                entry.into_id()
                            }
                        };
                        assert!(slab.capacity() == fixed::Capacity::Full);
                        assert_eq!(drops.get(), 0);
                        let remaining = turn.turn().app().remaining();
                        let mut entry = slab.entry(id).ok().expect("retained task");
                        assert_eq!(matches!(entry.get(), Deferred::Head(_)), complete);
                        let output = if take {
                            let (_, mut member) = entry.into_parts();
                            let DeferredProjection::Head(output) = member.value().project() else {
                                panic!("retained output");
                            };
                            let output = output.take().expect("one output");
                            member.discard();
                            Some(output)
                        } else {
                            entry.remove();
                            None
                        };
                        assert_eq!(turn.turn().app().remaining(), remaining);
                        assert_eq!(observed.get(), (1, true));
                        assert!(slab.capacity() == fixed::Capacity::Available);
                        assert_eq!(drops.get(), usize::from(!take));
                        drop(output);
                        assert_eq!(drops.get(), 1);
                    }
                });
            });
            assert_eq!(activity, (0, 0));
            let state = State::default();
            {
                let mut slab = pin!(fixed::Slab::<'_, _, 1, Domain>::new());
                Scope::with(slab.as_mut(), |mut slab| {
                    let mut id = slab
                        .insert(dope_fiber::abi::Task::new(Producer::new(&state, true, 7)))
                        .expect("stream slot");
                    for expected in [task::Poll::Pending, task::Poll::Ready(Ok(7))] {
                        let entry = slab.entry(id).ok().expect("live stream");
                        id = turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            let ControlFlow::Continue((entry, poll)) = Context::with_target(
                                root.target(),
                                share.work(),
                                driver.reborrow(),
                                |context| entry.poll_next(context),
                            ) else {
                                panic!("live stream");
                            };
                            assert_eq!(poll, expected);
                            assert_eq!(share.work().remaining(), 0);
                            entry.into_id()
                        });
                    }
                    let entry = slab.entry(id).ok().expect("live stream");
                    let ControlFlow::Break(ended) = Context::with_target(
                        root.target(),
                        turn.turn().app(),
                        driver.reborrow(),
                        |context| entry.poll_next(context),
                    ) else {
                        panic!("ended stream");
                    };
                    assert_eq!((state.polls.get(), state.drops.get()), (3, 1));
                    let id = ended.into_id();
                    assert!(slab.capacity() == fixed::Capacity::Full);
                    slab.entry(id).ok().expect("ended slot").remove();
                    assert!(slab.capacity() == fixed::Capacity::Available);
                });
            }
            assert_eq!(state.drops.get(), 1);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}
