use std::{cell::Cell, num::NonZero};

use dope_net::link::egress::{
    self,
    load::Value,
    metadata::{self, arena, batch},
};
use dope_test::alloc::Tracker;
use o3::base::cell::region;

pub mod common;

use common::chain::Payload;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retry_keeps_measurement() {
    let bytes = [1, 2, 3];
    let lengths = Cell::new(Some(0));
    let charges = Cell::new(Some(0));
    region::Token::scope(|mut token| {
        let store = arena::Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<1, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut store = (store).expect("required fixture outcome");
        let arena = arena::Arena::<Payload>::try_new(&token, &mut store);
        let arena = (arena).expect("required fixture outcome");
        let lane = arena.get(metadata::Lane::at(NonZero::<u32>::MIN));
        assert!(lane.is_some());
        let Some(lane) = lane else { return };
        let queue = lane.queue();
        let plan = batch::Plan::new(egress::Entries::new(1), egress::Resident::new(0));
        let plan = (plan).expect("required fixture outcome");

        let ((), allocs) = Tracker::usage(|| {
            let first = egress::NonEmpty::try_new(Payload {
                bytes: &bytes[..1],
                lengths: &lengths,
                charges: &charges,
            });
            let second = egress::NonEmpty::try_new(Payload {
                bytes: &bytes,
                lengths: &lengths,
                charges: &charges,
            });
            assert!(first.is_ok() && second.is_ok());
            let (Ok(first), Ok(second)) = (first, second) else {
                return;
            };
            assert_eq!((lengths.get(), charges.get()), (Some(2), Some(2)));

            let charge = second.charge();
            let failed = queue.try_batch(&mut token, plan, |mut sink| {
                sink.try_push(first)?;
                sink.try_push(second)
            });
            assert!(matches!(failed, Err(batch::Error::Build(_))));
            let Err(batch::Error::Build(rejected)) = failed else {
                return;
            };
            assert!(queue.is_empty());
            assert!(matches!(
                rejected.reason,
                egress::Rejection::BatchCapacity { requested, remaining, cause }
                    if requested == charge
                        && remaining == egress::Credit::fixed::<0, 0>()
                        && matches!(cause, egress::Shortage::Entries(deficit)
                            if *deficit.available() == egress::Entries::new(0)
                                && *deficit.requested() == egress::Entries::new(1))
            ));
            assert_eq!(
                rejected.load.geometry().wire().into_nonzero().get() as usize,
                bytes.len()
            );
            assert_eq!(rejected.load.as_inner().bytes.as_ptr(), bytes.as_ptr());
            assert_eq!((lengths.get(), charges.get()), (Some(2), Some(2)));

            let retry = queue.try_batch(&mut token, plan, |mut sink| sink.try_push(rejected.load));
            assert!(retry.is_ok());
            let front = queue.take_front(&mut token);
            assert!(front.is_some());
            let Some(front) = front else { return };
            assert_eq!(front.into_inner().bytes.as_ptr(), bytes.as_ptr());
            assert!(queue.is_empty());
            assert_eq!((lengths.get(), charges.get()), (Some(2), Some(2)));
        });
        assert_eq!(allocs, (0, 0));
    });
}

#[test]
fn admit_keeps_measurement() {
    let bytes = [1, 2, 3];
    let lengths = Cell::new(Some(0));
    let charges = Cell::new(Some(0));
    region::Token::scope(|mut token| {
        let store = arena::Store::try_new(
            egress::Config::shared(egress::Credit::fixed::<1, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut store = (store).expect("required fixture outcome");
        let arena = arena::Arena::<Payload>::try_new(&token, &mut store);
        let arena = (arena).expect("required fixture outcome");
        let lane = arena.get(metadata::Lane::at(NonZero::<u32>::MIN));
        assert!(lane.is_some());
        let Some(lane) = lane else { return };
        let queue = lane.queue();
        let plan = batch::Plan::new(egress::Entries::new(1), egress::Resident::new(0));
        let plan = (plan).expect("required fixture outcome");

        let ((), allocs) = Tracker::usage(|| {
            let first = egress::Load::new(Payload {
                bytes: &bytes[..1],
                lengths: &lengths,
                charges: &charges,
            });
            let second = egress::Load::new(Payload {
                bytes: &bytes,
                lengths: &lengths,
                charges: &charges,
            });
            assert!(matches!(
                queue.admit(&mut token, first),
                metadata::Admission::Committed
            ));
            let failed = queue.admit(&mut token, second);
            assert!(matches!(failed, metadata::Admission::Rejected(_)));
            let metadata::Admission::Rejected(rejected) = failed else {
                return;
            };
            let (load, reason) = rejected.into_parts();
            assert!(matches!(reason, egress::Rejection::Capacity { .. }));
            assert!(matches!(load, Value::Checked(_)));
            let Value::Checked(load) = load else { return };
            assert_eq!(load.as_inner().bytes.as_ptr(), bytes.as_ptr());
            assert_eq!((lengths.get(), charges.get()), (Some(2), Some(2)));

            {
                let front = queue.take_front(&mut token);
                assert!(front.is_some());
                let Some(front) = front else { return };
                assert_eq!(front.into_inner().bytes, &bytes[..1]);
            }
            let retry = queue.try_batch(&mut token, plan, |mut sink| sink.try_push(load));
            assert!(retry.is_ok());
            let front = queue.take_front(&mut token);
            assert!(front.is_some());
            let Some(front) = front else { return };
            assert_eq!(front.into_inner().bytes.as_ptr(), bytes.as_ptr());
            assert_eq!((lengths.get(), charges.get()), (Some(2), Some(2)));
            assert!(queue.is_empty());
        });
        assert_eq!(allocs, (0, 0));
    });
}
