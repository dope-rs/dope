use dope_core::driver::life::recovery;
use o3::depot::queue::resident;

use crate::common::wake::Ready as _;
pub mod common;

use dope_core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{
        self,
        ready::{
            wake,
            wake::{Drain, fanout, handoff, queue, table},
        },
    },
};

use crate::common::{
    rt::{Runtime, finish, recover},
    wake::{FOUR_LANES, MANY_LANES},
};

#[test]
fn exhaustion_keeps_owner() {
    const COUNT: usize = schedule::LIMIT + 1;
    let mut reports = Vec::new();

    Runtime::throughput().with_scope(
        |scope| {
            scope.with_driver(|driver, mut schedule| {
                let driver = driver.driver_ref();
                let targets = driver.targets::<KeyTag<0>>();
                let [q_slot, q_next, x_slot, f_slot] = [0_u16, 1, 2, 3].map(|slot| {
                    let target = targets
                        .bind(SlotIndex::from(slot), Epoch::INITIAL)
                        .dispatch();
                    driver.ready().make_slot(target).expect("ready slot")
                });
                let queued =
                    (queue::Queue::try_new(driver, 2, FOUR_LANES)).expect("queue registry");
                let exact = (table::Table::try_new(driver, 1)).expect("exact registry");
                let fanout = (fanout::Set::try_new(driver, COUNT)).expect("fanout registry");
                let mut q_wait = queue::Registration::new();
                let mut q_second = queue::Registration::new();
                let mut x_wait = table::Registration::new();
                let mut f_wait = [const { fanout::Registration::new() }; COUNT];

                assert!(matches!(
                    queued.try_register(3, &mut q_wait, &wake::Waker::from(q_slot.target())),
                    Ok(())
                ));
                assert!(matches!(
                    queued.try_register(3, &mut q_second, &wake::Waker::from(q_next.target())),
                    Ok(())
                ));
                assert!(matches!(
                    exact.try_register(0, &mut x_wait, &wake::Waker::from(x_slot.target())),
                    Ok(())
                ));
                for slot in &mut f_wait {
                    fanout
                        .try_register(slot, &wake::Waker::from(f_slot.target()))
                        .expect("fanout subscription");
                }
                queued.begin_drain();
                exact.begin_drain();
                fanout.publish();

                {
                    let turn = schedule.begin();
                    let work = turn.turn().app();
                    work.share::<{ schedule::LIMIT }, _>(|share| {
                        assert!(share.work().permit().is_some());
                        assert!(exact.drain_one(share.work()) == Drain::Exhausted);
                    });
                    assert_eq!(work.remaining(), schedule::LIMIT - 1);
                    assert!(turn.is_exhausted());
                    assert_eq!(turn.ready_round(|_| panic!("unexpected wake")), 0);
                }
                {
                    let turn = schedule.begin();
                    assert!(!turn.is_exhausted());
                    let work = turn.turn().app();
                    work.share::<2, _>(|share| {
                        for _ in 0..schedule::LIMIT / 2 {
                            assert!(fanout.drain_one(share.work()) == Drain::Woke);
                        }
                        assert!(fanout.drain_one(share.work()) == Drain::Exhausted);
                        assert!(queued.drain_one(share.work()) == Drain::Exhausted);
                    });
                    assert_eq!(work.remaining(), schedule::LIMIT / 2);
                    assert_eq!(turn.ready_round(|_| ()), 1);
                    assert!(!driver.ready().has_ready());
                    assert!(turn.is_exhausted());
                }
                assert!(queued.has_waiters());
                assert!(queued.is_draining());
                assert!(exact.has_waiters());
                assert!(q_wait.is_active());
                assert!(q_second.is_active());

                let turn = schedule.begin();
                assert!(!turn.is_exhausted());
                let work = turn.turn().app();
                for _ in schedule::LIMIT / 2..COUNT {
                    assert!(fanout.drain_one(work) == Drain::Woke);
                }
                assert!(fanout.drain_one(work) == Drain::Empty);
                assert!(!fanout.has_pending());
                assert!(queued.drain_one(work) == Drain::Woke);
                assert!(queued.drain_one(work) == Drain::Woke);
                assert!(queued.drain_one(work) == Drain::Empty);
                assert!(exact.drain_one(work) == Drain::Woke);
                assert!(exact.drain_one(work) == Drain::Empty);
                assert_eq!(turn.ready_round(|_| ()), 4);
                assert!(!turn.is_exhausted());
            })
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
#[test]
fn empty_costs_no_budget() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let registry = (queue::Queue::try_new(driver, 0, MANY_LANES)).expect("queue registry");
            let exact = (table::Table::try_new(driver, 4_096)).expect("exact table registry");
            let handoff = (handoff::Queue::<(), _>::try_new(driver, 0, resident::Fixed::<4_096>))
                .expect("handoff registry");
            let fanout = (fanout::Set::try_new(driver, 0)).expect("fanout set");
            let work = turn.turn().app();
            work.share::<{ schedule::LIMIT }, _>(|share| {
                let work = share.work();
                assert!(work.permit().is_some());
                registry.begin_drain();
                assert!((registry.drain_one(work)) == (Drain::Empty));
                assert!(!registry.is_draining());
                exact.begin_drain();
                assert!((exact.drain_one(work)) == (Drain::Empty));
                assert!(!exact.is_draining());
                handoff.begin_drain();
                assert!((handoff.drain_one(work)) == (Drain::Empty));
                fanout.publish();
                assert!((fanout.drain_one(work)) == (Drain::Empty));
                assert!(!fanout.has_pending());
            });
            assert_eq!(work.remaining(), schedule::LIMIT - 1);
            assert!(!turn.is_exhausted());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
