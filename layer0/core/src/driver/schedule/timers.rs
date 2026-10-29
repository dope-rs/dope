use o3::mem::quota;

use crate::{
    dope_sys::Budget as _,
    driver,
    driver::schedule::{clock::timer, state::lanes},
};

pub(super) enum Lane {}

pub(super) fn expire(driver: driver::Reference<'_>, ledger: &quota::Ledger<Lane>) {
    let timer = driver.timer();
    let mut budget = lanes::Budget::reserve_all(ledger);
    let now = driver.clock().now();
    loop {
        match budget.admit(|| match timer.expire_one(now) {
            timer::Expiry::Fired => Some(()),
            timer::Expiry::Pending => None,
        }) {
            quota::Admission::Item(()) => {}
            quota::Admission::Empty | quota::Admission::Exhausted => return,
        }
    }
}
