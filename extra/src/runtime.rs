use std::io;
use std::marker::PhantomData;

use dope::manifold::Manifold;
use dope_fiber::{AppSessionExt, Fiber};

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher<'d, M>
where
    M: Manifold<'d>,
{
    #[pin]
    #[manifold]
    manifold: M,
    brand: PhantomData<&'d ()>,
}

pub struct AppRuntime<'a, 'scope, 'd: 'scope, S, M>
where
    M: Manifold<'d> + 'd,
{
    app: dope::runtime::AppSession<'a, 'scope, 'd, S, Dispatcher<'d, M>>,
}

impl<'scope, 'd: 'scope, S, M> AppRuntime<'_, 'scope, 'd, S, M>
where
    M: Manifold<'d> + 'd,
{
    pub fn enter<R>(
        session: &mut dope::runtime::Session<'scope, 'd, S>,
        manifold: M,
        f: impl for<'a> FnOnce(AppRuntime<'a, 'scope, 'd, S, M>) -> R,
    ) -> R {
        session.with_app(
            Dispatcher {
                manifold,
                brand: PhantomData,
            },
            |app| f(AppRuntime { app }),
        )
    }

    pub fn block_on<F>(&mut self, fiber: F) -> io::Result<F::Output>
    where
        F: Fiber<'d>,
    {
        self.app.block_on(fiber)
    }
}
