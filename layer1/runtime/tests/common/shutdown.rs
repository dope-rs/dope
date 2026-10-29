use dope_core::driver::life::creation;
use dope_runtime::{
    executor::{self, Build as _, create},
    shutdown,
};

use crate::common::runtime::topology::PROFILE_CONFIG;

pub fn try_create(
    attempt: &mut creation::Attempt,
    pair: shutdown::Pair,
) -> Result<(executor::Executor<(), shutdown::Source>, shutdown::Trigger), create::Error<'_>> {
    let (source, trigger) = pair.split();
    let executor = source.try_build(attempt, PROFILE_CONFIG)?;
    Ok((executor, trigger))
}
