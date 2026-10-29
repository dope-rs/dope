pub mod common;

use std::num::NonZero;

use data::compose;
use dope_net::link::egress::{self, data, metadata, metadata::arena::Store};
use dope_test::alloc::Tracker;
use metadata::arena::Arena;
use o3::{base::cell::region, buffer::pool};

use crate::common::chain::{A, B, D, Fill as _, LANE, REJECTED};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn lane_reject_keeps_owner() {
    region::Token::scope(|mut token| {
        let bufs = (pool::Pool::try_new(1, D.len())).expect("cursor pool geometry must fit");
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<1, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena = Arena::<pool::Cursor>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = LANE;
        let slot = (arena.get(lane)).expect("metadata lane must exist");
        let queue = slot.queue();
        let value = bufs.try_cursor(D).expect("buffer fixture");
        let charge = egress::Charge::of(&value);

        let ((), alloc) = Tracker::usage(|| {
            let denied = queue.admit(&mut token, egress::Load::new(value));
            assert!(matches!(denied, metadata::Admission::Rejected(_)));
            if let metadata::Admission::Rejected(denied) = denied {
                assert!(matches!(
                    *denied.error(),
                    egress::Rejection::LaneCapacity { charge: got } if got == charge
                ));
                assert_eq!(denied.into_parts().0.into_inner().as_ref(), D);
            }
        });

        assert_eq!(alloc, (0, 0));
    });
}

#[test]
fn branch_keeps_owner() {
    region::Token::scope(|mut token| {
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<1, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena =
            Arena::<compose::Either<&'static [u8], &'static [u8]>>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = LANE;
        let slot = (arena.get(lane)).expect("metadata lane must exist");
        let queue = slot.queue();
        let first = egress::Load::new(B)
            .try_nonempty()
            .expect("nonempty first input");
        let left = egress::Load::new(REJECTED)
            .try_nonempty()
            .expect("nonempty left input");
        let right = egress::Load::new(REJECTED)
            .try_nonempty()
            .expect("nonempty right input");

        let ((), alloc) = Tracker::usage(|| {
            assert!(queue.try_right(&mut token, first).is_ok());
            let denied = queue.try_left(&mut token, left);
            assert!(denied.is_err(), "occupied branch must reject");
            let Err(denied) = denied else { return };
            assert!(
                denied.cause
                    == egress::Admission::Storage {
                        lane,
                        requested: egress::Credit::fixed::<1, 0>(),
                        available: egress::Entries::new(0),
                    }
            );
            let left = denied.load;
            assert_eq!(left.as_inner().as_ptr(), REJECTED.as_ptr());
            let front =
                (queue.take_front(&mut token)).expect("right branch entry must remain present");
            assert!(matches!(
                front.into_inner(),
                compose::Either::Right(value) if value.as_ptr() == B.as_ptr()
            ));

            assert!(queue.try_left(&mut token, left).is_ok());
            let denied = queue.try_right(&mut token, right);
            assert!(denied.is_err(), "occupied branch must reject");
            let Err(denied) = denied else { return };
            assert!(
                denied.cause
                    == egress::Admission::Storage {
                        lane,
                        requested: egress::Credit::fixed::<1, 0>(),
                        available: egress::Entries::new(0),
                    }
            );
            let right = denied.load;
            assert_eq!(right.as_inner().as_ptr(), REJECTED.as_ptr());
            let front =
                (queue.take_front(&mut token)).expect("left branch entry must remain present");
            assert!(matches!(
                front.into_inner(),
                compose::Either::Left(value) if value.as_ptr() == REJECTED.as_ptr()
            ));
            assert!(queue.try_right(&mut token, right).is_ok());
            let front =
                (queue.take_front(&mut token)).expect("retried right branch must remain present");
            assert!(matches!(front.into_inner(), compose::Either::Right(value)
                if value.as_ptr() == REJECTED.as_ptr()));
        });

        assert_eq!(alloc, (0, 0));
    });
}

#[test]
fn front_restore_order() {
    region::Token::scope(|mut token| {
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<2, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena = Arena::<&[u8]>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = LANE;
        let slot = (arena.get(lane)).expect("metadata lane must exist");
        let queue = slot.queue();

        let ((), alloc) = Tracker::usage(|| {
            assert!(matches!(
                queue.admit(&mut token, egress::Load::new(A)),
                metadata::Admission::Committed
            ));
            assert!(matches!(
                queue.admit(&mut token, egress::Load::new(B)),
                metadata::Admission::Committed
            ));

            let front = (queue.take_front(&mut token)).expect("first entry must remain present");
            assert_eq!(front.get().as_ptr(), A.as_ptr());
            assert!(!queue.is_empty());
            front.restore();

            for want in [A, B] {
                let front =
                    (queue.take_front(&mut token)).expect("restored entry must remain present");
                let value = front.into_inner();
                assert_eq!(value.as_ptr(), want.as_ptr());
                assert_eq!(value.len(), want.len());
            }
            assert!(queue.is_empty());
        });

        assert_eq!(alloc, (0, 0));
    });
}
