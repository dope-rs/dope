use dope_core::driver::route::KeyTag;
use dope_manifold::timing::watchdog;
use o3::base::time::Positive;

type ID = KeyTag<47>;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct Host<'d> {
    #[pin]
    #[manifold]
    pub(crate) watchdog: watchdog::Watchdog<'d, ID>,
}

impl Host<'_> {
    pub fn config<const PERIOD: u64, const DEADLINE: u64>()
    -> Result<watchdog::Config, dope_manifold::ConfigError> {
        watchdog::Config::new(
            Positive::from_millis::<PERIOD>(),
            Positive::from_millis::<DEADLINE>(),
        )
    }
}
