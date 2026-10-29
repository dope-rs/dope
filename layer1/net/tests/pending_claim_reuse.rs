pub mod common;

use std::convert::Infallible;

use common::credit::{Error, open, release_one, with_pool};
use dope_core::{
    driver::{life::recovery, result::poll::Close, route::table::conn},
    io::Reclaimed,
};
use dope_net::link::{
    egress,
    pool::pending::{Disposition, Pending},
};
use dope_test::scope;

#[test]
fn claimed_slot_reuse() {
    with_pool::<2>(
        egress::Config::DEFAULT,
        |pool, [claimed, other], driver, turn| {
            let capacity = conn::Capacity::fixed::<2>();
            Pending::of(pool).at(claimed).expect("live claim").wake();
            Pending::of(pool)
                .try_round()
                .expect("pending round")
                .run(turn.maint(), |key| {
                    assert_eq!(key, claimed);
                    assert!(matches!(release_one(pool, key, driver, turn), Ok(false)));
                    assert!(matches!(
                        open(pool, capacity, driver),
                        Err(Error::Unavailable)
                    ));
                    assert!(matches!(release_one(pool, other, driver, turn), Ok(false)));
                    let next = open(pool, capacity, driver).expect("unclaimed vacancy");
                    assert_eq!(next.lane(), other.lane());
                    Disposition::Gone
                });
            let next = open(pool, capacity, driver).expect("released claim");
            assert_eq!(next.lane(), claimed.lane());
        },
        recovery::Policy::new(
            |report| {
                assert!(
                    matches!(
                        report,
                        Reclaimed::Close {
                            slot,
                            file: Close::Closed(Ok(())),
                            reactor: Ok(()),
                        } if slot.route().is_framework()
                    ),
                    "unexpected reclaimed operation: {report:?}"
                );
            },
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("driver recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("native lifecycle");
            result.ok().expect("driver output").expect("credit fixture");
        },
    );
}
