use crate::{
    executor::{self, session},
    topology::lending,
};

/// Executes the constructed application without exposing installation authority.
pub trait Execute<'life, 'd: 'life, O, Q>: lending::Build<'d, O>
where
    O: lending::Root<'d> + executor::raw::App<'d>,
{
    type Output;

    fn run<'app, 'run>(
        data: Self::Data,
        app: session::App<'run, 'life, 'd, lending::raw::Bound<'app, 'd, O, Self::App<'app>>, Q>,
    ) -> Self::Output
    where
        Self: 'app,
        O: 'app,
        O::Provider: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run;
}
