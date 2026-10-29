use std::os::fd;

use o3::base::num::bounded;

use crate::{
    io::reactor::complete,
    ring::register::{Occupied, Remove},
};

/// Synchronous registration preserves exact syscall failures and slot owners.
pub trait Registry {
    type Token;
    type Target;
    type File: Occupied<Target = Self::Target>;
    type Cancel;
    type Files;
    type Range;
    type Update;

    fn cancel_all(&mut self) -> Result<(), Self::Cancel>;
    fn cancel(
        &mut self,
        token: Self::Token,
    ) -> Result<bounded::U32<0, { i32::MAX as u32 }>, Self::Cancel>
    where
        Self::Token: complete::raw::Token;
    fn sparse_files<const MAX: u32>(
        &mut self,
        count: bounded::U32<1, MAX>,
    ) -> Result<(), Self::Files>;
    fn file_range<const MAX: u32>(&mut self, len: bounded::U32<1, MAX>) -> Result<(), Self::Range>;
    fn install(&mut self, slot: Self::Target, file: fd::BorrowedFd<'_>)
    -> Result<(), Self::Update>;
    fn occupy(
        &mut self,
        slot: Self::Target,
        file: fd::BorrowedFd<'_>,
    ) -> Result<Self::File, Self::Update>;
    fn try_remove(&mut self, file: Self::File) -> Result<(), Remove<Self::File, Self::Update>>;
}
