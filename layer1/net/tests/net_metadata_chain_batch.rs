pub mod common;

use std::{cell::Cell, convert::Infallible, num::NonZero};

use dope_net::link::egress::{self, metadata, metadata::arena::Store};
use dope_test::alloc::Tracker;
use metadata::{arena::Arena, batch};
use o3::{base::cell::region, buffer::pool};

use crate::common::chain::{A, B, C, D, Expected as _, Fill as _, LANE, REJECTED};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn rollback_reuses() {
    region::Token::scope(|mut token| {
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<4, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena = Arena::<&[u8]>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = arena.get(LANE).expect("lane");
        let queue = lane.queue();

        let ((), alloc) = Tracker::usage(|| {
            assert!(matches!(
                queue.admit(&mut token, egress::Load::new(A)),
                metadata::Admission::Committed
            ));
            let plan = batch::Plan::new(egress::Entries::new(2), egress::Resident::new(0));
            let plan = (plan).expect("required fixture outcome");
            let b = egress::NonEmpty::try_new(B).expect("nonempty B");
            let c = egress::NonEmpty::try_new(C).expect("nonempty C");
            let result = queue.try_batch(&mut token, plan, |mut sink| {
                sink.try_push(b)?;
                sink.try_push(c)?;
                Ok::<(), batch::Rejected<&'static [u8]>>(())
            });
            assert!(result.is_ok());
            for want in [A, B, C] {
                let front = queue.take_front(&mut token).expect("published owner");
                let value = front.into_inner();
                assert_eq!(value.as_ptr(), want.as_ptr());
                assert_eq!(value.len(), want.len());
            }

            let plan = batch::Plan::new(egress::Entries::new(3), egress::Resident::new(0));
            let plan = (plan).expect("required fixture outcome");
            let rolled = queue.try_batch(&mut token, plan, |mut sink| {
                for value in [A, B] {
                    let load = egress::NonEmpty::try_new(value).expect("nonempty owner");
                    assert!(sink.try_push(load).is_ok());
                }
                Ok::<_, Infallible>(())
            });
            assert!(matches!(
                rolled,
                Err(batch::Error::Mismatch { remaining })
                    if remaining == egress::Credit::fixed::<1, 0>()
            ));
            assert!(queue.is_empty());

            for value in [A, B, C, D] {
                assert!(matches!(
                    queue.admit(&mut token, egress::Load::new(value)),
                    metadata::Admission::Committed
                ));
            }
            let denied = queue.admit(&mut token, egress::Load::new(REJECTED));
            let metadata::Admission::Rejected(denied) = denied else {
                panic!("full queue must reject admission");
            };
            assert!(*denied.error() == LANE.capacity::<1, 0>(egress::Charge::of(&REJECTED)));
            let denied = denied.into_parts().0.into_inner();
            assert_eq!(denied.as_ptr(), REJECTED.as_ptr());
            assert_eq!(denied.len(), REJECTED.len());

            for want in [A, B, C, D] {
                let front = queue.take_front(&mut token).expect("published owner");
                assert_eq!(front.into_inner().as_ptr(), want.as_ptr());
            }
            assert!(queue.is_empty());
        });

        assert_eq!(alloc, (0, 0));
    });
}

#[test]
fn admission_skips_build() {
    region::Token::scope(|mut token| {
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<1, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena = Arena::<&[u8]>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = arena.get(LANE).expect("lane");
        let queue = lane.queue();

        let ((), alloc) = Tracker::usage(|| {
            let plan = batch::Plan::new(egress::Entries::new(2), egress::Resident::new(0));
            let plan = (plan).expect("required fixture outcome");
            let called = Cell::new(false);
            let denied: Result<(), batch::Error<Infallible>> =
                queue.try_batch(&mut token, plan, |_| {
                    called.set(true);
                    Ok(())
                });
            assert!(!called.get());
            let Err(batch::Error::Admission(cause)) = denied else {
                panic!("batch admission must fail before build");
            };
            assert!(cause == LANE.credit::<2, 0>(1));
            assert!(queue.is_empty());
        });

        assert_eq!(alloc, (0, 0));
    });
}

#[test]
fn error_rolls_back() {
    region::Token::scope(|mut token| {
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<2, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena = Arena::<&[u8]>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = arena.get(LANE).expect("lane");
        let queue = lane.queue();

        let ((), alloc) = Tracker::usage(|| {
            let plan = batch::Plan::new(egress::Entries::new(2), egress::Resident::new(0));
            let plan = (plan).expect("required fixture outcome");
            let failed = queue.try_batch(&mut token, plan, |mut sink| {
                for value in [A, B] {
                    let load = egress::NonEmpty::try_new(value).expect("nonempty owner");
                    assert!(sink.try_push(load).is_ok());
                }
                Err::<(), _>(7_u8)
            });
            assert!(matches!(failed, Err(batch::Error::Build(7))));
            assert!(queue.is_empty());

            for value in [C, D] {
                assert!(matches!(
                    queue.admit(&mut token, egress::Load::new(value)),
                    metadata::Admission::Committed
                ));
            }
            for want in [C, D] {
                let front = queue.take_front(&mut token).expect("published owner");
                assert_eq!(front.into_inner().as_ptr(), want.as_ptr());
            }
        });

        assert_eq!(alloc, (0, 0));
    });
}

#[test]
fn resident_once() {
    region::Token::scope(|mut token| {
        let bufs = pool::Pool::try_new(2, D.len());
        let bufs = (bufs).expect("required fixture outcome");
        let bytes = 2 * D.len();
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::new(
                egress::Entries::new(2),
                egress::Resident::new(bytes as u32),
            )),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena = Arena::<pool::Cursor>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = arena.get(LANE).expect("lane");
        let queue = lane.queue();

        let ((), alloc) = Tracker::usage(|| {
            let plan =
                batch::Plan::new(egress::Entries::new(2), egress::Resident::new(bytes as u32));
            let plan = (plan).expect("required fixture outcome");
            let a = bufs.try_cursor(A).expect("buffer fixture");
            let b = bufs.try_cursor(B).expect("buffer fixture");
            let a = egress::NonEmpty::try_new(a).expect("nonempty A");
            let b = egress::NonEmpty::try_new(b).expect("nonempty B");
            let result = queue.try_batch(&mut token, plan, |mut sink| {
                sink.try_push(a)?;
                sink.try_push(b)?;
                Ok::<_, batch::Rejected<pool::Cursor>>(11)
            });
            assert_eq!(result.ok().expect("batch callback"), 11);
        });
        assert_eq!(alloc, (0, 0));
        for want in [A, B] {
            let front = queue.take_front(&mut token).expect("published owner");
            assert_eq!(front.into_inner().as_ref(), want);
        }
        assert!(queue.is_empty());
    });
}
