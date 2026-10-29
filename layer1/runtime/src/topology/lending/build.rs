use dope_core::driver;

use crate::{client, executor, topology::lending};

/// Constructs one dependent application after all installation storage is ready.
///
/// The executor retains the unique session throughout construction and execution.
/// No client can escape through `Data`, `Error`, or the execution output.
pub trait Build<'d, O>: Sized
where
    O: lending::Root<'d> + executor::raw::App<'d>,
{
    type App<'app>: executor::raw::App<'d>
    where
        Self: 'app,
        O: 'app,
        O::Provider: 'app,
        'd: 'app;
    type Data;
    type Error;

    fn build<'app>(
        self,
        client: <O::Provider as client::Provider<'d>>::Client<'app>,
        driver: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        O: 'app,
        O::Provider: 'app,
        'd: 'app;
}
