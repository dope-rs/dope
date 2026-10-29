use dope_core::driver::schedule::app;

use crate::client::session::response::work::Reserve;

pub trait Seal {}

impl Seal for app::Work<'_, '_> {}

impl Seal for Reserve<'_, '_> {}
