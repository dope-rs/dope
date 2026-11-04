use std::{collections::HashSet, num::NonZero};

use dope_core::driver::{Binding, life::recovery, schedule::ready::task::owner};

pub mod common;
use common::rt::{Runtime, finish, recover};

#[test]
fn interleaves_issuance() {
    let mut events = Vec::new();
    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            let driver = scope.context().driver_ref();
            let binding = Binding::new(driver.timer(), owner::Capacity::new(NonZero::<u32>::MIN));
            let mut issued = HashSet::new();
            for _ in 0..3 {
                assert!(issued.insert(driver.try_next_id().expect("singleton")));
                let first = binding.sequence();
                assert!(issued.insert(driver.try_next_id().expect("singleton")));
                let second = binding.sequence();
                for _ in 0..3 {
                    assert!(issued.insert(first.try_next().expect("first block")));
                    assert!(issued.insert(driver.try_next_id().expect("singleton")));
                    assert!(issued.insert(second.try_next().expect("second block")));
                }
            }
        },
        recovery::Policy::new(|event| events.push(event), recover),
        finish,
    );
    assert!(events.is_empty(), "unexpected reclamation: {events:?}");
}
