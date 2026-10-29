use std::{convert::Infallible, marker::PhantomData};

use dope_core::driver;
use dope_runtime::{client::Provider, executor, topology::lending};

struct Escape<'d, O, A>(A, PhantomData<&'d O>);

impl<'d, O, A> lending::Build<'d, O> for Escape<'d, O, A>
where
    O: lending::Root<'d> + executor::raw::App<'d> + 'd,
    O::Provider: 'd,
    A: executor::raw::App<'d>,
{
    type App<'app>
        = A
    where
        Self: 'app,
        O: 'app,
        O::Provider: 'app,
        'd: 'app;
    type Data = <O::Provider as Provider<'d>>::Client<'d>;
    type Error = Infallible;

    fn build<'app>(
        self,
        client: <O::Provider as Provider<'d>>::Client<'app>,
        _: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        O: 'app,
        O::Provider: 'app,
        'd: 'app,
    {
        Ok((self.0, client))
    }
}

fn main() {}
