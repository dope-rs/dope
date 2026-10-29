use crate::{
    driver::{
        self,
        life::{domain::runtime, native::create},
        result, settings,
        storage::resident::Arena,
    },
    random,
};

/// Stack-resident construction recovery; a rejected native owner remains here until reclaimed.
pub struct Attempt {
    failure: Option<result::CreateError>,
}

impl Default for Attempt {
    fn default() -> Self {
        Self::new()
    }
}

impl Attempt {
    pub const fn new() -> Self {
        Self { failure: None }
    }

    /// Consumes one configuration and fresh entropy; a failed attempt keeps its cleanup authority.
    pub fn try_driver(
        &mut self,
        config: settings::Config,
    ) -> Result<driver::Driver, &mut result::CreateError> {
        self.try_build(config, random::Seed::acquire)
    }

    /// Consumes the supplied seed exactly once without deriving or copying its key material.
    pub fn try_seed(
        &mut self,
        config: settings::Config,
        seed: random::Seed,
    ) -> Result<driver::Driver, &mut result::CreateError> {
        self.try_build(config, || Ok(seed))
    }

    fn try_build(
        &mut self,
        config: settings::Config,
        seed: impl FnOnce() -> Result<random::Seed, crate::Errno>,
    ) -> Result<driver::Driver, &mut result::CreateError> {
        let failure = match &mut self.failure {
            Some(error) => return Err(error),
            slot @ None => slot,
        };
        let plan = match config.into_plan() {
            Ok(plan) => plan,
            Err(error) => return Err(failure.insert(result::CreateError::Plan(error))),
        };
        let seed = match seed() {
            Ok(seed) => seed,
            Err(error) => {
                return Err(failure.insert(result::CreateError::kernel(
                    result::IoOperation::Entropy,
                    create::Kernel::from(error).0,
                )));
            }
        };
        let resident = match Arena::try_new(plan.config().domains().get() as usize) {
            Ok(resident) => resident,
            Err(error) => return Err(failure.insert(error.into())),
        };
        let (shared, backend) = driver::Shared::try_new(seed, plan, failure)?;
        Ok(driver::Driver {
            runtime: runtime::Runtime::new(backend, resident, shared),
        })
    }
}
