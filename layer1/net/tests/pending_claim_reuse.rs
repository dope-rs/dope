pub mod common;

use common::credit::{Error, open, release_one, with_pool};
use dope_core::driver::route::table::conn;
use dope_net::link::{
    egress,
    pool::pending::{Disposition, Pending},
};

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
    );
}
