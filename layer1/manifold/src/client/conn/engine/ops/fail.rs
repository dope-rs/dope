use app::failure::{self, Controlled, Observed};
use dope_core::driver::{retained, route::Identity};
use dope_net::link::event;
use o3::depot::arena::slab::key;

use crate::{
    client::{
        app,
        attempt::{self, queue},
    },
    service::{self, discover, sync},
};

pub(super) trait Fail<'d, ID: Identity, A, S, T>: failure::Mode + Sized
where
    A: app::Application<'d, ID, Failure = Self>,
    S: attempt::Control<'d, T, ID>,
    T: dope_net::Transport,
{
    fn fail(
        ctrl: &mut S,
        app: &mut A,
        owner: attempt::Id<'d, ID>,
        cause: event::ConnectError,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'q, 'h, 'd, ID: Identity + 'd, A, T, R, I, const N: usize, F>
    Fail<'d, ID, A, service::Pool<'q, 'h, 'd, ID, T, R, I, N, F>, T> for Controlled
where
    A: app::Application<'d, ID, Failure = Self>,
    T: dope_net::Transport,
    R: sync::Policy,
    I: Eq,
    F: discover::Feedback,
{
    fn fail(
        ctrl: &mut service::Pool<'q, 'h, 'd, ID, T, R, I, N, F>,
        _: &mut A,
        owner: attempt::Id<'d, ID>,
        cause: event::ConnectError,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let now = driver.driver_ref().clock().now();
        ctrl.fail(owner, cause, now);
    }
}

impl<'q, 'h, 'd, ID: Identity + 'd, A, T, R, I, const N: usize, F>
    Fail<'d, ID, A, service::Pool<'q, 'h, 'd, ID, T, R, I, N, F>, T> for Observed
where
    A: app::Application<'d, ID, Failure = Self> + app::Failed<'d, ID>,
    T: dope_net::Transport,
    R: sync::Policy,
    I: Eq,
    F: discover::Feedback,
{
    fn fail(
        ctrl: &mut service::Pool<'q, 'h, 'd, ID, T, R, I, N, F>,
        app: &mut A,
        owner: attempt::Id<'d, ID>,
        cause: event::ConnectError,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let now = driver.driver_ref().clock().now();
        ctrl.fail(owner, cause, now);
        app::Failed::failed(
            app,
            attempt::Completion::reported(owner, ctrl),
            event::ConnectFault::Transport(cause),
            driver,
        );
    }
}

impl<'source, 'd, ID: Identity + 'd, A, T> Fail<'d, ID, A, queue::Control<'source, 'd, T, ID>, T>
    for Observed
where
    A: app::Application<'d, ID, Failure = Self> + app::Failed<'d, ID>,
    T: dope_net::Transport,
{
    fn fail(
        ctrl: &mut queue::Control<'source, 'd, T, ID>,
        app: &mut A,
        owner: attempt::Id<'d, ID>,
        cause: event::ConnectError,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let (owner, cause) = match ctrl.failed(owner) {
            Ok(attempt::Transition::Applied) => (owner, event::ConnectFault::Transport(cause)),
            Ok(attempt::Transition::Stale) => return,
            Err(error) => {
                let (owner, error) = error.into_event();
                let cause = match error {
                    event::AttemptError::Lookup(
                        key::Observation::Free { .. }
                        | key::Observation::Generation { .. }
                        | key::Observation::Retired { .. },
                    ) => return,
                    error => event::ConnectFault::Cleanup { cause, error },
                };
                (owner, cause)
            }
        };
        app::Failed::failed(
            app,
            attempt::Completion::reported(owner, ctrl),
            cause,
            driver,
        );
    }
}
