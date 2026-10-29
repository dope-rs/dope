use std::convert;

use dope_core::driver::{route, settings};
use dope_runtime::executor;

use crate::app::dispatch;

/// A route capability deliberately absent from one installed owner.
pub struct Omit;

impl<'d, M> dispatch::raw::Installs<'d, M> for Omit {
    fn call<'app, 'owner>(
        install: executor::raw::Install<'app, 'owner, 'd, M>,
    ) -> executor::raw::Install<'app, 'owner, 'd, M> {
        install
    }
}

impl<'d, M> executor::raw::Finishes<'d, M> for Omit {
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, M>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, M> {
        finish
    }
}

unsafe impl<'d, M, R: route::App> dispatch::raw::Wakes<'d, M, R> for Omit {
    type Plan = convert::Infallible;

    const ROUTES: settings::Routes = settings::Routes::EMPTY;

    fn select() -> Option<Self::Plan> {
        None
    }

    unsafe fn call<'a, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'a,
            M,
            Self::Plan,
            executor::raw::Activate<'driver, 'turn, 'root, 'd>,
        >,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        call.into_never()
    }
}
