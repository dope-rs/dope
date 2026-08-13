use crate::driver;

pub trait Backend {}

impl Backend for driver::Context<'_, '_> {}
