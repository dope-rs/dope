use crate::driver::{
    self,
    schedule::ready::{self, wake::Once},
};

pub(in crate::driver::schedule::ready::wake) mod drain;
pub(in crate::driver) mod slot;

pub(super) fn one<'d>(driver: driver::Reference<'d>, target: Option<ready::Retained<'d>>) -> Once {
    match target {
        None => Once::Empty,
        Some(target) => {
            target.wake(driver);
            Once::Woke
        }
    }
}
