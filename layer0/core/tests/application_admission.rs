use std::convert::Infallible;

use dope_core::driver::life::{creation, recovery};
use o3::mem::quota;
pub mod common;

use std::cell::Cell;

use app::step;
use dope_core::{
    driver,
    driver::{life::domain, settings},
};
use driver::{schedule, schedule::app};

use crate::common::{contract::admit::Admission as _, rt::Runtime};

#[test]
fn is_atomic() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let mut creation = creation::Attempt::new();
    let driver = creation
        .try_driver(config)
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.with_driver(|_, mut schedule| {
                        {
                            let active = schedule.begin();
                            let work = active.turn().app();
                            let calls = Cell::new(0);

                            let empty = work.claim(|| {
                                calls.set(calls.get() + 1);
                                None::<usize>
                            });
                            assert!(matches!(empty, quota::Admission::Empty));
                            assert_eq!(work.remaining(), schedule::LIMIT);

                            for _ in 0..schedule::LIMIT {
                                let admitted = work.claim(|| {
                                    calls.set(calls.get() + 1);
                                    Some(7)
                                });
                                let (value, permit) = (admitted.into_item()).expect(
                                    "the application claim must consume its available admission",
                                );
                                assert_eq!(value, 7);
                                let direct = step::Receipt::direct(permit);
                                assert!((direct.settle(step::Flow::Batch)) == (step::Flow::Batch));
                            }
                            assert_eq!(work.remaining(), 0);

                            let blocked = work.claim(|| {
                                calls.set(calls.get() + 1);
                                Some(11)
                            });
                            assert!(matches!(blocked, quota::Admission::Exhausted));
                            assert_eq!(calls.get(), schedule::LIMIT + 1);
                        }

                        {
                            let active = schedule.begin();
                            let work = active.turn().app();
                            let rearmed = Cell::new(0);
                            let taken = Cell::new(0);
                            for item in 0..schedule::LIMIT {
                                let ready = step::Checkpoint::new(
                                    || {
                                        taken.set(taken.get() + 1);
                                        item
                                    },
                                    || rearmed.set(rearmed.get() + 1),
                                );
                                assert_eq!(step::Step::new(work).batch(ready), Some(item));
                            }
                            let ready = step::Checkpoint::new(
                                || {
                                    taken.set(taken.get() + 1);
                                    23
                                },
                                || rearmed.set(rearmed.get() + 1),
                            );
                            assert_eq!(step::Step::new(work).batch(ready), None);
                            assert_eq!(taken.get(), schedule::LIMIT);
                            assert_eq!(rearmed.get(), 1);
                        }

                        {
                            let active = schedule.begin();
                            let work = active.turn().app();
                            assert_eq!(work.remaining(), schedule::LIMIT);

                            let permit = (work.permit())
                                .expect("the delegated claim must consume an available admission");
                            let (observed, receipt) =
                                step::Receipt::delegate(permit, |proof| proof.run(|| 17));
                            assert_eq!(observed, 17);
                            let yielded = receipt.settle(step::Flow::Yield);
                            assert!((yielded) == (step::Flow::Yield));
                            assert_eq!(work.remaining(), schedule::LIMIT - 1);
                        }

                        {
                            let active = schedule.begin();
                            let work = active.turn().maint();
                            let empty = work.claim(|| None::<usize>);
                            assert!(matches!(empty, quota::Admission::Empty));
                            assert_eq!(work.remaining(), schedule::LIMIT);

                            let admitted = work.claim(|| Some(13));
                            let (value, permit) = (admitted.into_item()).expect(
                                "the maintenance claim must consume an available admission",
                            );
                            assert_eq!(permit.run(|| value), 13);
                            assert_eq!(work.remaining(), schedule::LIMIT - 1);

                            let inner = work.share::<2, _>(|| {
                                assert_eq!(work.remaining(), schedule::LIMIT / 2);
                                let empty = work.claim(|| None::<usize>);
                                assert!(matches!(empty, quota::Admission::Empty));
                                assert_eq!(work.remaining(), schedule::LIMIT / 2);

                                let admitted = work.claim(|| Some(17));
                                let (value, permit) = (admitted.into_item()).expect(
                                "the shared maintenance claim must consume an available admission",
                            );
                                assert_eq!(permit.run(|| value), 17);
                                work.remaining()
                            });
                            assert_eq!(inner, schedule::LIMIT / 2 - 1);
                            assert_eq!(work.remaining(), schedule::LIMIT - 2);
                            while let Some(permit) = work.permit() {
                                permit.run(|| {});
                            }
                            let called = Cell::new(false);
                            let blocked = work.claim(|| {
                                called.set(true);
                                Some(19)
                            });
                            assert!(matches!(blocked, quota::Admission::Exhausted));
                            assert!(!called.get());
                        }

                        {
                            let active = schedule.begin();
                            let work = active.turn().maint();
                            for left in (0..schedule::LIMIT).rev() {
                                let permit = work.permit().expect("maintenance capacity");
                                permit.run(|| assert_eq!(work.remaining(), left));
                            }
                            assert!(work.permit().is_none());
                        }
                    });
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}
