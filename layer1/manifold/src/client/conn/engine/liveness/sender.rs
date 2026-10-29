use dope_core::driver::{retained, route::Identity, schedule::ready};
use dope_net::link::{pool, slot::transmit};

use crate::{
    app::env,
    client::{
        self, app,
        conn::engine::{self, liveness},
        side::mode::Mode,
    },
};

pub(in crate::client::conn::engine) struct Sender<'a, 'p, 'w, 'd: 'p + 'w, ID: Identity, A, E, X>
where
    A: app::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    pub(super) native: transmit::Sender<
        'a,
        'p,
        'w,
        'd,
        ID,
        A::Wire,
        engine::Record<'d, ID, A>,
        A::Send,
        { client::IOV_CAP },
    >,
    pub(super) liveness:
        &'a mut liveness::State<'d, ID, <E::Transport as dope_net::Transport>::Prepared, X>,
    pub(super) key: pool::Key<'d, ID>,
    pub(super) target: ready::Target<'d>,
}
impl<'a, 'p, 'w, 'd: 'p + 'w, ID: Identity, A, E, X> Sender<'a, 'p, 'w, 'd, ID, A, E, X>
where
    A: app::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    pub(in crate::client::conn::engine) fn view(
        &mut self,
    ) -> liveness::Egress<'_, 'p, 'w, 'd, ID, A, E, X> {
        let view = self.native.view();
        // SAFETY: the native sender and book loan were produced together by their owner.
        let source =
            unsafe { engine::raw::reunite(&mut *self.liveness, view.connection, self.key) };
        liveness::Egress {
            flights: view.flights,
            connection: liveness::Change::new(source, self.target),
            pending: view.pending,
            queue: view.queue,
        }
    }

    pub(in crate::client::conn::engine) fn submit(
        &mut self,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> transmit::Attempt {
        self.native.submit(driver)
    }
}
