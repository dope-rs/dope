use o3::buffer::storage;
pub mod common;

use std::{convert::Infallible, num::NonZero};

use dope_net::link::egress::{self, metadata, metadata::arena::Store};
use dope_test::alloc::Tracker;
use metadata::arena::Arena;
use o3::{base::cell::region, buffer::pool};

use crate::common::chain::{A, B, D, Expected as _, Fill as _, LANE};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn commit_reenters() {
    region::Token::scope(|mut token| {
        let bufs = pool::Pool::try_new(2, D.len());
        let bufs = (bufs).expect("required fixture outcome");
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<2, { (2 * D.len()) as u32 }>()),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena = Arena::<pool::Cursor>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = LANE;
        let lane = arena.get(lane);
        assert!(lane.is_some());
        let Some(lane) = lane else { return };
        let queue = lane.queue();

        let (done, alloc) = Tracker::usage(|| {
            let mut done = None;
            let value = bufs.try_cursor(B).expect("buffer fixture");
            let load = egress::Load::new(value).try_nonempty();
            assert!(load.is_ok());
            if let Ok(load) = load {
                let reserve = queue.try_reserve(&mut token, load);
                assert!(matches!(reserve, metadata::Reservation::Ready(_)));
                if let metadata::Reservation::Ready(reserve) = reserve {
                    let result = reserve.try_with::<_, Infallible>(|token, value| {
                        assert_eq!(value.as_ref(), B);
                        let value = bufs.try_cursor(A).expect("buffer fixture");
                        assert!(matches!(
                            queue.admit(token, egress::Load::new(value)),
                            metadata::Admission::Committed
                        ));
                        Ok(7)
                    });
                    assert!(result.is_ok());
                    if let Ok(value) = result {
                        done = Some(value);
                    }
                }
            }
            done
        });
        assert_eq!(done, Some(7));
        assert_eq!(alloc, (0, 0));
        for want in [A, B] {
            let front = queue.take_front(&mut token);
            assert!(front.is_some());
            let Some(front) = front else { return };
            assert_eq!(front.into_inner().as_ref(), want);
        }
        assert!(queue.is_empty());
    });
}

#[test]
fn drop_returns_reservation() {
    region::Token::scope(|mut token| {
        let bufs = pool::Pool::try_new(1, D.len()).expect("buffer fixture");
        let mut stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<1, { D.len() as u32 }>()),
            NonZero::<usize>::MIN,
        )
        .expect("metadata fixture");
        let arena = Arena::<pool::Cursor>::try_new(&token, &mut stored).expect("arena fixture");
        let lane = arena.get(LANE).expect("lane fixture");
        let queue = lane.queue();
        let ((), alloc) = Tracker::usage(|| {
            for _ in 0..3 {
                let owner = bufs.try_cursor(B).expect("returned buffer");
                let load = egress::Load::new(owner)
                    .try_nonempty()
                    .ok()
                    .expect("nonempty");
                {
                    let result = queue.try_reserve(&mut token, load);
                    assert!(matches!(result, metadata::Reservation::Ready(_)));
                }
                assert!(queue.is_empty());
            }
            let owner = bufs.try_cursor(A).expect("returned buffer");
            assert!(matches!(
                queue.admit(&mut token, egress::Load::new(owner)),
                metadata::Admission::Committed
            ));
            assert_eq!(
                queue
                    .take_front(&mut token)
                    .expect("published owner")
                    .into_inner()
                    .as_ref(),
                A
            );
        });
        assert_eq!(alloc, (0, 0));
    });
}

#[test]
fn reservation_returns_all() {
    region::Token::scope(|mut token| {
        let bufs = pool::Pool::try_new(2, D.len());
        let bufs = (bufs).expect("required fixture outcome");
        let stored = Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<1, { D.len() as u32 }>()),
            NonZero::<usize>::MIN,
        );
        let mut stored = (stored).expect("required fixture outcome");
        let arena = Arena::<pool::Cursor>::try_new(&token, &mut stored);
        let arena = (arena).expect("required fixture outcome");
        let lane = LANE;
        let lane = arena.get(lane);
        assert!(lane.is_some());
        let Some(lane) = lane else { return };
        let queue = lane.queue();

        let ((), alloc) = Tracker::usage(|| {
            let value = bufs.try_cursor(b"").expect("buffer fixture");
            let empty = egress::Load::new(value).try_nonempty();
            assert!(empty.is_err());
            if let Err(rejected) = empty {
                let empty = egress::Empty::try_from(rejected);
                assert!(empty.is_ok());
                if let Ok(empty) = empty {
                    let (empty, storage::Zero) = empty.into_parts();
                    assert!((empty.charge().bytes()) == (egress::WireBytes::ZERO));
                    assert!(empty.into_inner().as_ref().is_empty());
                }
            }

            let value = bufs.try_cursor(B).expect("buffer fixture");
            let load = egress::Load::new(value).try_nonempty();
            assert!(load.is_ok());
            if let Ok(load) = load {
                let result = {
                    let reserve = queue.try_reserve(&mut token, load);
                    assert!(matches!(reserve, metadata::Reservation::Ready(_)));
                    let metadata::Reservation::Ready(reserve) = reserve else {
                        return;
                    };
                    reserve.try_with(|token, value| {
                        assert_eq!(value.as_ref(), B);
                        let value = bufs.try_cursor(A).expect("buffer fixture");
                        let charge = egress::Charge::of(&value);
                        let rejected = queue.admit(token, egress::Load::new(value));
                        assert!(matches!(rejected, metadata::Admission::Rejected(_)));
                        if let metadata::Admission::Rejected(rejected) = rejected {
                            assert!(*rejected.error() == LANE.capacity::<1, 4>(charge));
                            let expected = *rejected.error();
                            let (load, reason) = rejected.into_parts();
                            assert!(reason == expected);
                            assert_eq!(load.into_inner().as_ref(), A);
                        }
                        Err::<(), _>(7)
                    })
                };
                assert!(result.is_err());
                if let Err(failure) = result {
                    let (value, cause) = failure.into_parts();
                    let pointer = value.as_inner().as_ref().as_ptr();
                    assert_eq!(value.as_inner().as_ref(), B);
                    assert_eq!(value.geometry().wire().widen().get(), B.len());
                    assert_eq!(cause, 7);
                    {
                        let retry = queue.try_reserve(&mut token, value);
                        assert!(matches!(retry, metadata::Reservation::Ready(_)));
                        let metadata::Reservation::Ready(retry) = retry else {
                            return;
                        };
                        retry.commit(|_| ());
                    }
                    let front = queue.take_front(&mut token);
                    assert!(front.is_some());
                    let Some(front) = front else { return };
                    assert_eq!(front.get().as_ref().as_ptr(), pointer);
                    assert_eq!(front.into_inner().as_ref(), B);
                }
            }

            let value = bufs.try_cursor(A).expect("buffer fixture");
            assert!(matches!(
                queue.admit(&mut token, egress::Load::new(value)),
                metadata::Admission::Committed
            ));
            let front = queue.take_front(&mut token);
            assert!(front.is_some());
            let Some(front) = front else { return };
            assert_eq!(front.into_inner().as_ref(), A);
        });
        assert_eq!(alloc, (0, 0));
        assert!(queue.is_empty());
    });
}
