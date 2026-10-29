use o3::depot::queue::resident::Take;

use crate::driver::{
    self,
    schedule::ready::{self, wake::Once},
};

pub(in crate::driver::schedule::ready::wake) mod drain;
pub(in crate::driver) mod slot;

pub(super) fn one<'d>(driver: driver::Reference<'d>, target: Take<ready::Retained<'d>>) -> Once {
    match target {
        Take::Empty => Once::Empty,
        Take::Item(target) => {
            target.wake(driver);
            Once::Woke
        }
    }
}
