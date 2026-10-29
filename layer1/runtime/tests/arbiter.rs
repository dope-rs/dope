use o3::mem::quota;
pub mod common;
use std::{array, mem};

use dope_runtime::executor::work;

use crate::common::work::Slot;

#[test]
fn round_robin() {
    assert_eq!(mem::size_of::<work::Arbiter<Slot>>(), 1);

    const READY: [Slot; 3] = [Slot::Zero, Slot::One, Slot::Two];
    let mut arb = work::Arbiter::<Slot>::new();
    let got = array::from_fn::<_, 7, _>(|turn| arb.select(|slot| slot <= READY[turn.min(2)]));
    assert!(
        got == [
            Some(Slot::Zero),
            Some(Slot::One),
            Some(Slot::Two),
            Some(Slot::Zero),
            Some(Slot::One),
            Some(Slot::Two),
            Some(Slot::Zero)
        ]
    );
}

#[test]
fn skips_idle() {
    let mut arb = work::Arbiter::<Slot>::new();
    assert!(arb.select(|slot| slot == Slot::Two) == Some(Slot::Two));
    arb.account(Slot::Zero);
    assert!(arb.select(|slot| slot > Slot::Three).is_none());
    assert!(arb.select(|slot| matches!(slot, Slot::One | Slot::Three)) == Some(Slot::One));
    assert!(arb.select(|slot| matches!(slot, Slot::One | Slot::Three)) == Some(Slot::Three));
}

#[test]
fn accounts_source() {
    let mut arb = work::Arbiter::<Slot>::new();
    let got = arb.admit(|source| match source {
        Slot::Two => quota::Admission::Item(7),
        Slot::Zero | Slot::One | Slot::Three => quota::Admission::Empty,
    });
    assert!(matches!(got, quota::Admission::Item(7)));
    let full = arb.admit::<()>(|source| match source {
        Slot::Zero | Slot::One | Slot::Two | Slot::Three => quota::Admission::Exhausted,
    });
    assert!(matches!(full, quota::Admission::Exhausted));
    assert!(arb.take() == Slot::Three);
    assert!(arb.take() == Slot::Zero);
}
