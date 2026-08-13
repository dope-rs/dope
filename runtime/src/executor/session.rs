use std::{io, marker, pin, process, time};

use dope_core::driver::{self, lifecycle};
use o3::cell::brand;

use crate::{client, executor, random, run, shutdown};

const APP_SHUTDOWN_DRAIN: time::Duration = time::Duration::from_secs(2);

pub struct Session<'scope, 'd: 'scope, S = (), Q = ()> {
    storage: pin::Pin<&'d S>,
    core: &'scope mut Core<'d, Q>,
    lifecycle: &'scope Q,
}

pub(super) struct Core<'d, Q> {
    scope: lifecycle::Scope<'d>,
    seed: random::Seed,
    retained_events: crate::Events<'d>,
    _shutdown: marker::PhantomData<fn(Q) -> Q>,
}

pub struct Application<'app, 'd: 'app, D, Q = ()> {
    core: &'app mut Core<'d, Q>,
    installed: crate::Installed<'app, 'd, D>,
}

struct AppScope<'app, 'd: 'app, D, Q>
where
    D: executor::Application<'d>,
{
    core: &'app mut Core<'d, Q>,
    installed: crate::Installed<'app, 'd, D>,
}

impl<'app, 'd: 'app, D, Q> AppScope<'app, 'd, D, Q>
where
    D: executor::Application<'d>,
{
    fn shutdown(&mut self) -> executor::raw::Pending<'app, 'd, D> {
        let installed = self.installed;
        let core = &mut *self.core;
        let completions = &mut core.retained_events;
        core.scope.with_turn(|token, mut driver, turn| {
            run::Run::new(&mut driver, installed, token, turn, completions).shutdown()
        })
    }

    fn drain(&mut self, pending: &executor::raw::Pending<'app, 'd, D>) -> io::Result<()> {
        let installed = self.installed;
        let core = &mut *self.core;
        let completions = &mut core.retained_events;
        core.scope.with_turn(|token, mut driver, turn| {
            run::Run::new(&mut driver, installed, token, turn, completions)
                .drain_for(APP_SHUTDOWN_DRAIN, pending)
        })
    }

    fn finalize(&mut self, pending: executor::raw::Pending<'app, 'd, D>) {
        let core = &mut *self.core;
        drop(core.retained_events.take());
        let (token, finalization) = match core.scope.final_quiescence() {
            Ok(finalization) => finalization,
            Err(_) => process::abort(),
        };
        let finish = pending.finish(finalization);
        self.installed.finish(token, finish);
    }

    fn finish(&mut self) -> io::Result<()> {
        let pending = self.shutdown();
        if self.drain(&pending).is_err() {
            process::abort();
        }
        self.finalize(pending);
        Ok(())
    }
}

impl<Q> Drop for Core<'_, Q> {
    fn drop(&mut self) {
        drop(self.retained_events.take());
        if self.scope.final_quiescence().is_err() {
            process::abort();
        }
    }
}

impl<'scope, 'd: 'scope, S, Q> Session<'scope, 'd, S, Q> {
    pub(super) fn new(
        storage: pin::Pin<&'d S>,
        core: &'scope mut Core<'d, Q>,
        lifecycle: &'scope Q,
    ) -> Self {
        Self {
            storage,
            core,
            lifecycle,
        }
    }

    /// Storage for the complete driver domain.
    /// Its domain lifetime is independent of the temporary session borrow.
    pub fn storage(&self) -> &'d S
    where
        S: 'd,
    {
        self.storage.get_ref()
    }

    /// Returns the session's exclusive driver context for staging resources.
    ///
    /// The access lifetime is tied to this mutable session borrow. Consequently
    /// the context cannot escape the generative executor scope, overlap another
    /// driver context, or coexist with an installed application.
    ///
    /// ```compile_fail
    /// use dope_core::driver;
    /// use dope_runtime::executor::Executor;
    ///
    /// fn escape(executor: Executor) -> driver::Context<'static, 'static> {
    ///     executor.enter(|mut session| session.driver_access())
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// use dope_runtime::executor::{self, Application};
    ///
    /// fn overlap<'scope, 'd, D>(
    ///     session: &mut executor::session::Session<'scope, 'd>,
    ///     app: D,
    /// ) where
    ///     'd: 'scope,
    ///     D: Application<'d>,
    /// {
    ///     let driver = session.driver_access();
    ///     let _ = session.with_app(app, |_| {});
    ///     drop(driver);
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// use dope_runtime::executor::session::Session;
    ///
    /// fn duplicate<'scope, 'd>(session: &mut Session<'scope, 'd>)
    /// where
    ///     'd: 'scope,
    /// {
    ///     let first = session.driver_access();
    ///     let second = session.driver_access();
    ///     drop((first, second));
    /// }
    /// ```
    pub fn driver_access(&mut self) -> driver::Context<'_, 'd> {
        self.core.scope.context()
    }

    /// Issues a keyed hash builder bound to this exact driver domain.
    ///
    /// ```compile_fail
    /// use dope_runtime::{
    ///     executor::Executor,
    ///     random::{Domain, HashState},
    /// };
    ///
    /// fn escape(executor: Executor) -> HashState<'static> {
    ///     executor.enter(|mut session| session.hash_state(Domain::new(0)))
    /// }
    /// ```
    pub fn hash_state(&mut self, domain: random::Domain) -> random::HashState<'d> {
        let seed = self.core.seed;
        seed.bind(self.core.scope.token(), domain)
    }

    fn token(&mut self) -> &mut brand::Token<'d> {
        self.core.scope.token()
    }

    /// Installs one application for `f` and finalizes it on normal return.
    /// # Panics
    /// Application or callback panics are terminal; session reuse is not guaranteed.
    pub fn with_app<D, R>(
        &mut self,
        app: D,
        f: impl for<'app> FnOnce(Application<'app, 'd, D, Q>) -> R,
    ) -> io::Result<R>
    where
        D: executor::Application<'d>,
        Q: executor::Startup,
    {
        let (output, finalized) = {
            let cell = pin::pin!(brand::Value::new(app));
            debug_assert!(self.core.retained_events.is_none());
            let installed = crate::Installed::install(cell.as_ref(), self.token());
            let mut scope = AppScope {
                core: &mut *self.core,
                installed,
            };
            self.lifecycle.installed();
            let output = f(Application {
                core: &mut *scope.core,
                installed: scope.installed.reborrow(),
            });
            let finalized = scope.finish();
            (output, finalized)
        };
        let reaped = self.core.scope.reap_finalized();
        if reaped.is_err() {
            process::abort();
        }
        finalized?;
        Ok(output)
    }

    /// Runs a composition with a pinned provider owner and its scoped client.
    /// The higher-ranked application lifetime prevents either from escaping.
    pub fn with_provider<O, P, C>(
        &mut self,
        owner: O,
        select: impl for<'borrow> FnOnce(pin::Pin<&'borrow O>) -> pin::Pin<&'borrow P>,
        composition: C,
    ) -> C::Output
    where
        P: client::Provider<'d>,
        C: client::Composition<'scope, 'd, S, Q, O, P>,
    {
        let mut owner = pin::pin!(owner);
        let client = P::provide(select(owner.as_ref()), client::Scope::new());
        let root = client::Anchor::new(owner.as_mut());
        C::compose(composition, client, root, self)
    }
}

impl<'app, 'd: 'app, D, Q> Application<'app, 'd, D, Q>
where
    D: executor::Application<'d>,
{
    pub fn drive<R>(&mut self, root: R) -> io::Result<R::Output>
    where
        R: executor::Root<'d>,
    {
        let installed = self.installed;
        let core = &mut *self.core;
        let completions = &mut core.retained_events;
        core.scope.with_turn(|token, mut driver, turn| {
            run::Run::new(&mut driver, installed, token, turn, completions).block_on(root)
        })
    }

    /// Issues a provider client tied to the surrounding application scope.
    pub fn client<P>(
        &mut self,
        select: impl for<'borrow> FnOnce(pin::Pin<&'borrow D>) -> pin::Pin<&'borrow P>,
    ) -> P::Client<'app>
    where
        'd: 'app,
        P: client::Provider<'d>,
    {
        let provider = select(self.installed.borrow_pin(self.core.scope.token()));
        P::provide(provider, client::Scope::new())
    }

    pub fn run(&mut self) -> io::Result<shutdown::Requested<Q>> {
        let installed = self.installed;
        let core = &mut *self.core;
        let completions = &mut core.retained_events;
        core.scope.with_turn(|token, mut driver, turn| {
            run::Run::new(&mut driver, installed, token, turn, completions).run()
        })?;
        Ok(shutdown::Requested::new())
    }
}

impl<'d, Q> Core<'d, Q> {
    pub(super) fn new(scope: lifecycle::Scope<'d>, seed: random::Seed) -> Self {
        Self {
            scope,
            seed,
            retained_events: None,
            _shutdown: marker::PhantomData,
        }
    }
}
