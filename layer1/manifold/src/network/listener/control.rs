use dope_core::driver::route::Identity;
use dope_net::link::{egress, pool::send};
use o3::{base::cell::region, depot::capacity};

use crate::{
    app::{dispatch, env},
    network::{
        listener,
        listener::{conn, handler},
    },
};

#[repr(transparent)]
pub struct Control<'step, 'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E>
where
    'd: 'step,
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    pub(in crate::network::listener) inner:
        dispatch::raw::Installed<'step, 'd, listener::Listener<'n, 'p, 's, 'w, 'd, ID, A, E>>,
}

impl<'step, 'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, E> Control<'step, 'n, 'p, 's, 'w, 'd, ID, A, E>
where
    'd: 'step,
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    pub fn handler_mut(&mut self) -> <A as listener::raw::Control<'d, ID>>::View<'_>
    where
        A: listener::raw::Control<'d, ID>,
    {
        let app = self.inner.project(|server| server.project().app);
        <A as listener::raw::Control<'d, ID>>::view(app)
    }

    pub fn close_after(&mut self, id: conn::Id<'d, ID>) {
        let this = self.inner.as_mut().project();
        if let Some(slot) = this.owner.pool_mut().get_mut(id.key()) {
            slot.close_after();
            slot.io().wake_target().wake();
        }
    }

    pub fn len(&self) -> capacity::Count {
        self.inner
            .as_ref()
            .project_ref()
            .owner
            .pool()
            .inspect()
            .len()
    }

    pub fn with_conn<R>(
        &mut self,
        id: conn::Id<'d, ID>,
        apply: impl FnOnce(&mut A::Conn) -> R,
    ) -> Option<R> {
        let this = self.inner.as_mut().project();
        let slot = this.owner.pool_mut().get_mut(id.key())?;
        Some(apply(&mut slot.state.conn))
    }

    pub fn wake(&mut self, id: conn::Id<'d, ID>) {
        let this = self.inner.as_mut().project();
        let Some(slot) = this.owner.pool_mut().get_mut(id.key()) else {
            return;
        };
        slot.io().wake_target().wake();
    }

    pub fn close(&self, id: conn::Id<'d, ID>) {
        self.inner.as_ref().get_ref().close(id);
    }

    pub fn mark_send(
        &self,
        region: &mut region::Token<region::Identity<'d>>,
        id: conn::Id<'d, ID>,
        bytes: egress::NonEmpty<A::Egress>,
    ) -> Result<(), send::Rejected<A::Egress>> {
        self.inner.as_ref().get_ref().mark_send(region, id, bytes)
    }
}
