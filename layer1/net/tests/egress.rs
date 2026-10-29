use dope_net::link::egress::{self, data, data::compose::Either};
use dope_test::alloc::Tracker;
use o3::buffer::{
    self,
    resident::{self, bound},
    storage,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn charges_shared() {
    let bytes = b"borrowed-egress".to_vec();
    let borrowed = resident::Shared::from(bytes.as_slice());
    assert!(egress::Charge::of(&borrowed).resident() == egress::ResidentSize::ZERO);

    let shared = (storage::Shared::try_copy(b"shared-egress"))
        .expect("the shared egress fixture must fit storage");
    let expected = buffer::Residency::resident(&shared);
    let shared = resident::Shared::from(shared);
    assert_eq!(egress::Charge::of(&shared).resident().get(), expected);

    let bytes = b"HTTP/1.1 100 Continue\r\n\r\n";
    let (fixed, alloc) = Tracker::usage(|| egress::NonEmpty::from_array(bytes));
    assert_eq!(alloc, (0, 0));
    assert_eq!(fixed.as_inner().as_ref().as_ptr(), bytes.as_ptr());
    assert_eq!(fixed.geometry().wire().widen().get(), bytes.len());
    assert!(fixed.charge().resident() == egress::ResidentSize::ZERO);
    let left = fixed.into_left::<resident::Shared<'_>>();
    assert_eq!(left.geometry().wire().widen().get(), bytes.len());
    assert!(left.charge().resident() == egress::ResidentSize::ZERO);
}

#[test]
fn rejection_retains_owner() {
    let shared = (storage::Shared::try_copy(b"retained-response"))
        .expect("the response fixture must fit storage");
    let pointer = shared.as_slice().as_ptr();
    let load = egress::Load::new(shared);
    let charge = load.charge();
    assert!(charge.resident().get() > 0);
    let (result, alloc) = Tracker::usage(|| load.try_bound::<bound::AtMost<0>>());
    assert_eq!(alloc, (0, 0));
    assert!(result.is_err(), "owned alloc must exceed zero backing");
    let Err(excess) = result else {
        return;
    };
    assert_eq!(excess.maximum, 0);
    assert!(excess.load.charge() == charge);
    assert_eq!(excess.load.as_inner().as_slice().as_ptr(), pointer);
    assert_eq!(excess.load.as_inner().as_slice(), b"retained-response");
    let (retry, alloc) = Tracker::usage(|| excess.load.try_bound::<bound::AtMost<4096>>());
    assert_eq!(alloc, (0, 0));
    retry
        .as_ref()
        .map_err(|excess| (excess.load.charge(), excess.maximum))
        .expect("the retained response must fit its larger bound");
    let Ok(bounded) = retry else {
        return;
    };
    assert!(bounded.charge() == charge);
    let shared = bounded.into_inner().into_inner();
    assert_eq!(shared.as_slice().as_ptr(), pointer);
}

#[test]
fn unbounded_moves_owner() {
    let bytes = b"borrowed-response";
    let load = egress::Load::new(bytes.as_slice());
    let charge = load.charge();
    let (bounded, alloc) = Tracker::usage(|| load.unbounded());
    assert_eq!(alloc, (0, 0));
    assert!(bounded.charge() == charge);
    let source = bounded.into_inner().into_inner();
    assert_eq!(source.as_ptr(), bytes.as_ptr());
    assert_eq!(source, bytes);
}

#[test]
fn preserves_bound_layout() {
    use std::mem;

    assert_eq!(
        mem::size_of::<egress::Excess<resident::Shared<'static>>>(),
        mem::size_of::<egress::Load<resident::Shared<'static>>>() + mem::size_of::<usize>(),
    );
    assert_eq!(
        mem::size_of::<egress::Load<data::Bound<resident::Shared<'static>, bound::AtMost<4096>>>>(),
        mem::size_of::<egress::Load<resident::Shared<'static>>>(),
    );
}

#[test]
fn nonempty_rejection() {
    let shared =
        storage::Shared::try_copy(b"bound-owner").expect("the response fixture must fit storage");
    let pointer = shared.as_slice().as_ptr();
    let load = egress::NonEmpty::try_new(shared).expect("the response is nonempty");
    let count = load.geometry().wire();
    let charge = load.charge();
    let (result, alloc) = Tracker::usage(|| load.try_bound::<bound::AtMost<0>>());
    assert_eq!(alloc, (0, 0));
    assert!(result.is_err());
    let Err(rejected) = result else { return };
    let (load, maximum) = rejected.into_parts();
    assert_eq!(maximum, egress::ResidentSize::ZERO);
    assert_eq!(load.geometry().wire(), count);
    assert_eq!(load.charge(), charge);
    assert_eq!(load.as_inner().as_slice().as_ptr(), pointer);
    let (result, alloc) = Tracker::usage(|| load.try_bound::<bound::AtMost<4096>>());
    assert_eq!(alloc, (0, 0));
    assert!(result.is_ok());
    let Ok(bound) = result else { return };
    let (split, alloc) = Tracker::usage(|| {
        bound
            .bound_right::<resident::Shared<'_>>()
            .into_unbound()
            .split()
    });
    assert_eq!(alloc, (0, 0));
    let load = match split {
        Either::Right(load) => Some(load),
        Either::Left(_) => None,
    }
    .expect("right payload");
    assert_eq!(load.geometry().wire(), count);
    assert_eq!(load.charge(), charge);
    assert_eq!(load.as_inner().as_slice().as_ptr(), pointer);
    assert_eq!(load.into_inner().as_slice(), b"bound-owner");
}

#[test]
fn nonempty_unbounded() {
    let shared = storage::Shared::try_copy(b"unbounded-owner")
        .expect("the response fixture must fit storage");
    let pointer = shared.as_slice().as_ptr();
    let load = egress::NonEmpty::try_new(shared).expect("the response is nonempty");
    let count = load.geometry().wire();
    let charge = load.charge();
    let (result, alloc) = Tracker::usage(|| load.try_bound::<bound::Unbounded>());
    assert_eq!(alloc, (0, 0));
    assert!(result.is_ok());
    let Ok(bound) = result else { return };
    let (load, alloc) = Tracker::usage(|| bound.into_unbound());
    assert_eq!(alloc, (0, 0));
    assert_eq!(load.geometry().wire(), count);
    assert_eq!(load.charge(), charge);
    assert_eq!(load.into_inner().as_slice().as_ptr(), pointer);
}

#[test]
fn nonempty_sum_variant() {
    for right in [false, true] {
        let bytes = b"borrowed-owner";
        let load = egress::NonEmpty::from_array(bytes);
        let count = load.geometry().wire();
        let charge = load.charge();
        let (result, alloc) = Tracker::usage(|| {
            let load = if right {
                load.into_right::<resident::Shared<'_>>()
            } else {
                load.into_left::<resident::Shared<'_>>()
            };
            load.try_bound::<bound::AtMost<0>>()
        });
        assert_eq!(alloc, (0, 0));
        assert!(result.is_ok());
        let Ok(bound) = result else { return };
        let (split, alloc) = Tracker::usage(|| bound.into_unbound().split());
        assert_eq!(alloc, (0, 0));
        let load = match split {
            Either::Left(load) => {
                assert!(!right);
                load
            }
            Either::Right(load) => {
                assert!(right);
                load
            }
        };
        assert_eq!(load.geometry().wire(), count);
        assert_eq!(load.charge(), charge);
        assert_eq!(load.as_inner().as_ref().as_ptr(), bytes.as_ptr());
    }
}
