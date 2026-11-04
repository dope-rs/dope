use o3::mem::quota;

use crate::{dope_sys::Budget as _, driver, driver::schedule::state::lanes};

pub(super) enum Lane {}

pub(super) fn expire(driver: driver::Reference<'_>, ledger: &quota::Ledger<Lane>) {
    let timer = driver.timer();
    let mut budget = lanes::Budget::reserve_all(ledger);
    let now = driver.clock().now();
    loop {
        match budget.admit(|| timer.expire_one(now)) {
            quota::Admission::Item(()) => {}
            quota::Admission::Empty | quota::Admission::Exhausted => return,
        }
    }
}
