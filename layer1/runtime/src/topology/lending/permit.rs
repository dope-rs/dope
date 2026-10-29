use std::{marker, pin};

use dope_core::driver::{self, schedule::clock::Sample};

use crate::{
    executor::{self, drive, drive::clock, session},
    topology::lending,
};

/// Affine construction and execution access for one exact provider and session.
pub struct Permit<'issued, 'life: 'issued, 'd: 'life, O, Q> {
    core: &'issued mut session::Core<'life, 'd, Q>,
    owner: pin::Pin<&'issued mut O>,
    start: Sample,
    source: marker::PhantomData<fn(&'issued ()) -> &'issued ()>,
}

impl<'issued, 'life: 'issued, 'd: 'life, O, Q> Permit<'issued, 'life, 'd, O, Q> {
    pub(crate) fn new(
        core: &'issued mut session::Core<'life, 'd, Q>,
        owner: pin::Pin<&'issued mut O>,
        start: Sample,
    ) -> Self {
        Self {
            core,
            owner,
            start,
            source: marker::PhantomData,
        }
    }

    pub fn driver(&mut self) -> driver::Context<'_, 'd> {
        self.core.scope.context()
    }
}

impl<'issued, 'life: 'issued, 'd: 'life, O, Q> Permit<'issued, 'life, 'd, O, Q>
where
    O: lending::raw::Resume<'d>,
    Q: executor::Startup,
{
    /// Consumes construction authority and retires the dependent application.
    pub fn execute<A, E>(self, app: A) -> lending::Outcome<'issued, 'life, 'd, O, Q, E>
    where
        A: executor::raw::App<'d>,
    {
        self.run(app, (), |(), app| app.run())
    }

    /// Builds a dependent application and consumes its data in the same client scope.
    pub fn try_run<A, D, E, R>(
        mut self,
        build: impl FnOnce(driver::Context<'_, 'd>) -> Result<(A, D), E>,
        run: impl for<'run> FnOnce(
            D,
            session::App<'run, 'life, 'd, lending::raw::Bound<'issued, 'd, O, A>, Q>,
        ) -> R,
    ) -> lending::Outcome<'issued, 'life, 'd, O, Q, E, R>
    where
        A: executor::raw::App<'d>,
    {
        match build(self.driver()) {
            Ok((app, data)) => self.run(app, data, run),
            Err(error) => lending::Outcome::rejected(self.core, error, self.source),
        }
    }

    fn run<A, D, E, R>(
        self,
        app: A,
        data: D,
        run: impl for<'run> FnOnce(
            D,
            session::App<'run, 'life, 'd, lending::raw::Bound<'issued, 'd, O, A>, Q>,
        ) -> R,
    ) -> lending::Outcome<'issued, 'life, 'd, O, Q, E, R>
    where
        A: executor::raw::App<'d>,
    {
        let mut lent = session::Lent::new(&mut *self.core);
        let (parent, outcome, disarm) = lent.execute(
            self.owner,
            app,
            self.start,
            |scope| (run(data, scope.app()), clock::wall()),
            drive::tick,
        );
        lending::Outcome::executed(self.core, parent, outcome, disarm)
    }

    /// Returns the exact uninstalled provider after construction rejects its input.
    pub fn reject<E>(self, error: E) -> lending::Outcome<'issued, 'life, 'd, O, Q, E> {
        lending::Outcome::rejected(self.core, error, self.source)
    }
}
