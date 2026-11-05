use o3::mem::quota;

use crate::driver::{
    self,
    schedule::{
        app,
        ready::{
            self,
            wake::{Drain, fanout::storage::Step},
        },
    },
};

pub(in crate::driver::schedule::ready::wake) fn one<'d>(
    driver: driver::Reference<'d>,
    work: app::Work<'_, 'd>,
    mut pending: impl FnMut() -> bool,
    mut next: impl FnMut() -> Option<Step<ready::Retained<'d>>>,
) -> Drain {
    loop {
        if !pending() {
            let _ = next();
            return Drain::Empty;
        }
        match work.run(&mut next) {
            quota::Admission::Item(Step::Idle) => {}
            quota::Admission::Item(Step::Item(target)) => {
                target.wake(driver);
                return Drain::Woke;
            }
            quota::Admission::Empty => return Drain::Empty,
            quota::Admission::Exhausted => {
                work.defer();
                return Drain::Exhausted;
            }
        }
    }
}
