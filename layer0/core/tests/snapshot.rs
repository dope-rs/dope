use std::convert::Infallible;

use dope_core::driver::life::{creation, recovery};
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::driver::{life::domain, settings, storage};
use dope_test::alloc::Tracker;
use hold::scope;
use o3::buffer::{
    hold,
    resident::staging,
    write::{Sink as _, consume::Consume as _},
};

use crate::common::rt::Runtime;

#[test]
fn layout() {
    let config = Runtime::quic::<2, 64>()
        .config()
        .with_domains(settings::Domain::ONE);
    let mut creation = creation::Attempt::new();
    let driver = creation
        .try_driver(config)
        .map_err(|error| error.kind())
        .expect("the snapshot test driver must initialize");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut life, _, _| {
                    let ctx = life.context();
                    let (budget, alloc) =
                        Tracker::usage(|| ctx.storage.budget(storage::Resident::bytes(64)));
                    let budget =
                        (budget).expect("the snapshot driver must reserve 64 resident bytes");
                    assert_eq!(alloc, (0, 0));
                    let plan = staging::Plan::new(1, 32);
                    let layout = (plan.and_then(|plan| staging::Layout::new(1, plan, 0, 0)))
                        .expect("the receive layout must fit");
                    let pool = (staging::Pool::try_new(&budget, layout))
                        .expect("the driver budget must admit its receive pool");
                    let mut owner = (staging::Buf::try_from(&pool))
                        .expect("the receive pool must retain its buffer");
                    assert!(owner.write_slice(b"abcdef").is_ok());

                    let state = {
                        let (left, alloc) = Tracker::usage(|| {
                            assert_eq!(owner.as_ref(), b"abcdef");
                            assert_eq!(owner.consume_max(2), 2);
                            owner.len()
                        });
                        assert_eq!(alloc, (0, 0));
                        assert_eq!(left, 4);
                        let before = budget.resident();
                        let (held, alloc) = Tracker::usage(|| {
                            scope::Input::new(&owner, &budget)
                                .try_plan(1..3)
                                .map(|plan| plan.bind(hold::Policy::new(3, 1)).try_into())
                        });
                        let held = held
                            .expect("the snapshot range must satisfy its hold policy")
                            .expect("the snapshot range must satisfy its hold policy");
                        assert_eq!(alloc.0, 1);
                        assert_eq!(held.as_slice(), b"de");
                        assert_eq!(budget.resident(), before + 2);
                        (held, before)
                    };

                    let (held, before) = state;
                    assert_eq!(owner.len(), 4);
                    assert_eq!(owner.as_slice(), b"cdef");
                    assert_eq!(
                        Tracker::during(move || assert_eq!(held.as_slice(), b"de")),
                        (0, 0)
                    );
                    assert_eq!(budget.resident(), before);

                    assert_eq!(owner.consume_max(usize::MAX), 4);
                    assert_eq!(owner.len(), 0);
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
