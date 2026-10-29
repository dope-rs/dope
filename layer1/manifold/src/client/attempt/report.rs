use std::convert;

use dope_core::{driver, driver::route::Identity};
use dope_net::link::event;
use o3::depot::arena::slab::key;

use crate::client::{
    app,
    attempt::{self, queue, queue::storage},
};

pub(crate) trait Report<'d, ID: Identity, A> {
    fn report(
        self,
        key: attempt::Completion<'_, 'd, ID>,
        app: &mut A,
        driver: &mut driver::Context<'_, 'd>,
    );
    fn retire(
        self,
        key: attempt::Completion<'_, 'd, ID>,
        app: &mut A,
        driver: &mut driver::Context<'_, 'd>,
    );
}

impl<'d, ID: Identity, A> Report<'d, ID, A> for convert::Infallible {
    fn report(
        self,
        _: attempt::Completion<'_, 'd, ID>,
        _: &mut A,
        _: &mut driver::Context<'_, 'd>,
    ) {
        match self {}
    }

    fn retire(
        self,
        _: attempt::Completion<'_, 'd, ID>,
        _: &mut A,
        _: &mut driver::Context<'_, 'd>,
    ) {
        match self {}
    }
}

impl<'d, ID: Identity, A: app::Failed<'d, ID>> Report<'d, ID, A> for queue::Lookup<'d, ID> {
    fn report(
        self,
        key: attempt::Completion<'_, 'd, ID>,
        app: &mut A,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (id, cause) = self.into_event();
        let key = key.with_id(id);
        app.failed(key, event::ConnectFault::Attempt(cause), driver);
    }

    fn retire(
        self,
        key: attempt::Completion<'_, 'd, ID>,
        app: &mut A,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        match self {
            Self::Key(storage::Error {
                observed:
                    key::Observation::Free { .. }
                    | key::Observation::Generation { .. }
                    | key::Observation::Retired { .. },
                ..
            }) => {}
            cause => cause.report(key, app, driver),
        }
    }
}
