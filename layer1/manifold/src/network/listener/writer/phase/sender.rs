use dope_core::driver::{self, route::Identity};
use dope_net::link::{egress, pool, pool::pending::Handle, slot::transmit};
use o3::{depot, depot::index::Initialized};

use crate::network::listener::{
    self, handler,
    writer::{
        self,
        deadline::{self, Slot},
    },
};

type Source<'a, 'n, 'p, 's, 'w, 'd, ID, A> = transmit::Sender<
    'a,
    'n,
    'w,
    'd,
    ID,
    <A as handler::Application<'d, ID>>::Wire,
    writer::Record<'p, 's, 'd, ID, A>,
    writer::Payload<'p, 's, 'd, ID, <A as handler::Application<'d, ID>>::Egress>,
    { listener::IOV_CAP },
    writer::Reserve<'d, <A as handler::Application<'d, ID>>::Egress>,
>;

type Admitted<'a, 'n, 'p, 's, 'w, 'd, ID, A> = depot::raw::Proof<
    (
        (
            Source<'a, 'n, 'p, 's, 'w, 'd, ID, A>,
            &'a mut deadline::Schedule<'d, ID>,
        ),
        pool::Key<'d, ID>,
    ),
    Initialized<()>,
>;

pub(in crate::network::listener::writer) struct Sender<
    'a,
    'n,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    A: handler::Application<'d, ID>,
> {
    source: Source<'a, 'n, 'p, 's, 'w, 'd, ID, A>,
    schedule: &'a mut deadline::Schedule<'d, ID>,
}

impl<'a, 'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A> Sender<'a, 'n, 'p, 's, 'w, 'd, ID, A>
where
    A: handler::Application<'d, ID>,
{
    pub(in crate::network::listener::writer) fn view(
        &mut self,
    ) -> (
        &crate::Flights<'d, ID>,
        Slot<'_, 'p, 's, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
        Handle<'_, 'd, ID>,
        egress::Queue<
            '_,
            'n,
            'd,
            { listener::IOV_CAP },
            writer::Payload<'p, 's, 'd, ID, A::Egress>,
            writer::Reserve<'d, A::Egress>,
        >,
    ) {
        let pool::EgressMut {
            flights,
            connection: conn,
            pending,
            queue,
        } = self.source.view();
        let key = conn.key();
        (
            flights,
            // SAFETY: `view` lent the initialized transmit sender for this exact
            // key, and the writer's schedule is keyed by that lane.
            Slot::from(unsafe { depot::raw::Proof::new(((conn, &mut *self.schedule), key)) }),
            pending,
            queue,
        )
    }

    pub(in crate::network::listener::writer) fn submit(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
    ) -> transmit::Attempt {
        self.source.submit(driver)
    }
}

impl<'a, 'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A> From<Admitted<'a, 'n, 'p, 's, 'w, 'd, ID, A>>
    for Sender<'a, 'n, 'p, 's, 'w, 'd, ID, A>
where
    A: handler::Application<'d, ID>,
{
    fn from(proof: Admitted<'a, 'n, 'p, 's, 'w, 'd, ID, A>) -> Self {
        let ((source, schedule), _) = proof.into_inner();
        Self { source, schedule }
    }
}
