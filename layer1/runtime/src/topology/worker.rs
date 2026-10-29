use dope_core::driver::storage;

use crate::{
    executor::{self, boot, session},
    topology::{self, app, lending},
};

/// Generative worker scope after storage construction and before application install.
pub struct Worker<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, I: 'd, Q: 'd> {
    pub(super) session: session::Scope<'scope, 'life, 'p, 'd, S, Q>,
    pub(super) installer: I,
}

impl<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, I: 'd, Q: 'd>
    Worker<'scope, 'life, 'p, 'd, S, I, Q>
{
    pub(crate) const fn new(
        session: session::Scope<'scope, 'life, 'p, 'd, S, Q>,
        setup: I,
    ) -> Self {
        Self {
            session,
            installer: setup,
        }
    }
}

impl<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, I, Q> Worker<'scope, 'life, 'p, 'd, S, I, Q>
where
    S: storage::Plan + 'd,
    I: 'd,
    Q: executor::Startup + 'd,
{
    /// Borrows storage from the exact prepared domain.
    pub fn storage(&self) -> &'life S::Output<'p, 'd> {
        self.session.storage()
    }

    /// Consumes the retained specification in one lexical lending scope.
    pub fn lend<R>(
        mut self,
        compose: impl FnOnce(I, &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>) -> R,
    ) -> R {
        let mut stage = lending::Stage::new(&mut self.session);
        compose(self.installer, &mut stage)
    }

    /// Runs the retained installer and consumer inside their storage loans.
    pub fn with<N, R, E>(mut self, next: N) -> Result<R, E>
    where
        I: for<'stage> lending::Installer<
                lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
                N,
                Output = R,
                Error = E,
            >,
    {
        let mut stage = lending::Stage::new(&mut self.session);
        self.installer.install(&mut stage, next)
    }

    /// Drives a yielding installation before publishing the resulting application.
    /// Rejects existing native work and cancels the unpolled root on admission failure.
    pub fn install_with<A, R, E>(
        mut self,
        install: impl FnOnce(I, &topology::Boot<'_, 'scope, 'life, 'p, 'd, S, Q>) -> R,
    ) -> Result<app::Application<'scope, 'life, 'p, 'd, S, A, Q>, topology::InstallError<E>>
    where
        A: executor::raw::App<'d>,
        R: boot::Root<'d, Output = Result<A, E>>,
    {
        let root = {
            let stage = topology::Boot::new(&self.session);
            install(self.installer, &stage)
        };
        let app = self
            .session
            .drive(root)
            .map_err(topology::InstallError::Drive)?
            .map_err(topology::InstallError::Install)?;
        Ok(app::Application {
            session: self.session,
            app,
        })
    }
}

impl<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, I, Q> Worker<'scope, 'life, 'p, 'd, S, I, Q>
where
    S: storage::Plan + 'd,
    I: topology::Installer<S> + 'd,
    Q: executor::Startup + 'd,
{
    pub fn install(
        mut self,
    ) -> Result<app::Application<'scope, 'life, 'p, 'd, S, I::App<'life, 'p, 'd>, Q>, I::Error>
    {
        let app = {
            let mut stage = topology::Stage::new(&mut self.session);
            self.installer.install(&mut stage)?
        };
        Ok(app::Application {
            session: self.session,
            app,
        })
    }
}
