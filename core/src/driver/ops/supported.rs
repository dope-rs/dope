//! Backend contexts admitted to bootstrap operations.

use crate::driver;

pub trait Supported {}

impl Supported for driver::Context<'_, '_> {}
