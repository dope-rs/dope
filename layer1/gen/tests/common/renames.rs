use dope_test::harness::scenario::TIMEOUT;
use engine::core::driver::storage;
use fibers::{BlockOn as _, abi};

use crate::common::{
    fibers::{App, Dummy},
    macros::Text,
};

#[fibers::fiber_fn('d, crate = ::fibers)]
pub(crate) async fn attributed<'d>() {}

pub(crate) fn expression<'d>() -> impl abi::Fiber<'d, Output = ()> {
    fibers::fiber!('d, crate = ::fibers => async move {})
}

use engine::runtime::{
    executor,
    executor::{drive, session},
};

pub struct Check<'report, F> {
    pub(crate) report: &'report mut session::Completion,
    pub(crate) check: F,
}

impl<'a, 'report, F> executor::Op<storage::With<Text<'a>, u8>, ()> for Check<'report, F>
where
    F: FnOnce(&str, u8),
{
    type Output =
        Result<Result<(), drive::Error>, session::Error<'report, Result<(), drive::Error>>>;

    fn run<'scope, 'life, 'p, 'd>(
        self,
        mut session: session::Scope<'scope, 'life, 'p, 'd, storage::With<Text<'a>, u8>>,
    ) -> Self::Output
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        'life: 'scope,
        Self: 'd,
        storage::With<Text<'a>, u8>: 'd,
        (): 'd,
    {
        let domain = session.storage();
        (self.check)(domain.0.0, domain.1);
        session.with_quiesce(self.report, App { dummy: Dummy }, TIMEOUT, |mut app| {
            app.block_on(expression())?;
            app.block_on(attributed())
        })
    }
}
