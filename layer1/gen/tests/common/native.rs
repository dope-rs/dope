use engine::{
    core::driver::{life::creation::Attempt, settings},
    manifold::timing::Throughput,
    runtime::executor::{Executor, create},
};
pub fn try_executor(attempt: &mut Attempt) -> Result<Executor, create::Error<'_>> {
    Executor::new(attempt, settings::Config::for_profile::<Throughput>())
}
