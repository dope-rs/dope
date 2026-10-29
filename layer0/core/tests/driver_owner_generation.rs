use crate::common::driver::support::Stamp as _;
pub mod common;

use std::num::NonZero;

use dope_core::driver::{
    route::Epoch,
    schedule::ready::task::owner::{self, Generation, Lane},
};

#[test]
fn distinguishes_last() {
    let previous = (NonZero::new(u64::MAX - 1).expect("nonzero generation")).epoch();
    let terminal = (NonZero::new(u64::MAX).expect("last generation")).epoch();

    let before = Generation::from_route(previous);
    let last = Generation::from_route(terminal);
    assert!(before != last);
    assert_eq!(before.nonzero(), previous.get());
    assert_eq!(last.nonzero(), terminal.get());
    assert_eq!(
        Generation::from_route(Epoch::INITIAL).nonzero(),
        Epoch::INITIAL.get()
    );
}

#[test]
fn rejects_unstored_raw() {
    std::assert_matches!(Lane::try_from(u32::MAX).err(), Some(owner::Invalid));
    assert!(Lane::try_from(u32::MAX - 1).is_ok());
}

#[test]
fn owner_u32_bounds() {
    let Some(count) = NonZero::<usize>::MIN.checked_add(u32::MAX as usize) else {
        return;
    };
    assert!(matches!(
        owner::Capacity::try_from(count),
        Err(owner::Overflow { .. })
    ));
}
