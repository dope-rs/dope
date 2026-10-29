use std::pin;

use dope_core::driver::{route::Identity, schedule};
use dope_net::link::pool::pending;
use o3::mem::quota;

use crate::{
    app::env,
    client::{
        app,
        attempt::queue,
        close::{self, Directive},
        conn::engine,
        side::mode::Mode,
    },
};

impl<'p, 'source, 'w, 'd: 'w, ID: Identity + 'd, A, E, X>
    engine::Machine<'p, 'w, 'd, ID, A, queue::Control<'source, 'd, E::Transport, ID>, E, X>
where
    A: app::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    pub(super) fn cancel(mut self: pin::Pin<&mut Self>, turn: schedule::Turn<'_, 'd>) {
        loop {
            let work = turn.reborrow().maint();
            let (attempt, key) = match work.run(|| self.as_mut().project().controller.take()) {
                quota::Admission::Item(item) => item,
                quota::Admission::Empty | quota::Admission::Exhausted => return,
            };
            let this = self.as_mut().project();
            let Some((slot, handle)) = pending::Mut::of(this.links.pool_mut()).get(key) else {
                continue;
            };
            if slot.state.owner.attempt() == attempt {
                slot.state
                    .closing
                    .apply(Directive::reconnect(close::Reason::Local(
                        close::LocalOrigin::OperationCancelled,
                    )));
                handle.wake();
            }
        }
    }
}
