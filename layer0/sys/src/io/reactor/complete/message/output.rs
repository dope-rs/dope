use o3::base::num::bounded;

use crate::io::reactor::complete::message::{Cut, Error};

/// Parsed regions borrow exactly the original native completion frame.
pub trait Output<'a>: Sized {
    type Capacity: Copy + Eq + Into<bounded::U64<0, { u64::MAX }>>;
    type Length: Copy + Eq + Into<bounded::U64<0, { u64::MAX }>>;

    fn parse(buffer: &'a [u8]) -> Result<Self, Error<Self::Capacity, Self::Length>>;
    fn name_cut(&self) -> Cut;
    fn payload_cut(&self) -> Cut;
    fn name(&self) -> &'a [u8];
    fn payload(&self) -> &'a [u8];
}
