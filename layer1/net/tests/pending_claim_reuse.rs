pub mod common;

use common::credit::{Error, open, release_one, with_pool};
use dope_core::driver::{route::table::conn, schedule};
use dope_net::link::{
    egress,
    pool::pending::{Busy, Disposition, Pending},
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

#[test]
fn ready_round_preserves_grant_and_wake_order() {
    with_pool::<2>(egress::Config::DEFAULT, |pool, [a, b], _, turn| {
        let waiter = Pending::of(pool).at(a).expect("first connection");
        let other = Pending::of(pool).at(b).expect("second connection");
        assert!(Pending::of(pool).ready_round().is_none());
        let empty = Pending::of(pool).try_round().expect("empty round");
        waiter.wait_send(true);
        assert!(Pending::of(pool).ready_round().is_none());
        assert!(matches!(Pending::of(pool).try_round(), Err(Busy)));
        drop(empty);

        let promoted = Pending::of(pool).ready_round().expect("capacity waiter");
        other.wake();
        drop(promoted);
        let work = turn.maint();
        work.share::<{ schedule::LIMIT }, _>(|| {
            Pending::of(pool).ready_round().expect("restored round").run(work, |key| {
                assert_eq!(key, a);
                waiter.wake();
                Disposition::Idle
            });
        });
        let mut expected = [b, a].into_iter();
        Pending::of(pool).ready_round().expect("remaining work").run(work, |key| {
            assert_eq!(expected.next(), Some(key));
            Disposition::Idle
        });
        assert!(expected.next().is_none());
        assert!(waiter.has_capacity());
        assert!(Pending::of(pool).ready_round().is_none());
    });
}
