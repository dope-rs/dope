use dope::core::driver::schedule::ready::task::{
    owner,
    owner::{Generation, Lane},
};

#[derive(Clone, Copy)]
pub struct Stamp(pub(crate) owner::Lane, pub(crate) owner::Generation);

impl Stamp {
    pub fn try_new(lane: u32, stamp: u32) -> Result<Self, owner::Invalid> {
        Ok(Self(Lane::try_from(lane)?, Generation::from_u32(stamp)))
    }
}

#[derive(Clone, Copy)]
pub enum Tasks {}

impl owner::Identity for Stamp {
    fn parts(self) -> (owner::Lane, owner::Generation) {
        (self.0, self.1)
    }
}

impl owner::Source for Tasks {
    type Identity = Stamp;
}
