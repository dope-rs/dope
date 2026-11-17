use dope_core::driver::{route::Identity, schedule::app};
use dope_net::link::pool;
use o3::depot;

use crate::network::listener::{
    self, conn, handler,
    writer::{self, deadline::{self, Slot}, memory},
};

pub(in crate::network::listener) struct Egress<
    'e,
    'a,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    A: handler::Application<'d, ID>,
> {
    pub(in crate::network::listener) connection:
        Slot<'a, 'p, 's, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>,
    output: conn::Open<'a, 'p, 's, 'e, 'd, ID, A::Egress>,
}

impl<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, A> Egress<'e, 'a, 'p, 's, 'w, 'd, ID, A>
where
    A: handler::Application<'d, ID>,
{
    pub(in crate::network::listener) fn new(
        view: pool::EgressMut<
            'a, 'e, 'w, 'd, ID, A::Wire, writer::Record<'p, 's, 'd, ID, A>,
            writer::Payload<'p, 's, 'd, ID, A::Egress>, { listener::IOV_CAP },
            writer::Reserve<'d, A::Egress>,
        >,
        writes: &'p memory::Arena<'s, 'd, ID, A::Egress>,
        deadlines: &'a mut deadline::Schedule<'d, ID>,
    ) -> Self {
        let pool::EgressMut { flights, connection, pending, queue } = view;
        let key = connection.key();
        Self {
            // SAFETY: the original writer lends the resident and its paired deadline extent.
            connection: Slot::from(unsafe { depot::raw::Proof::new(((connection, deadlines), key)) }),
            output: conn::Open {
                slots: flights,
                permit: pending,
                held: memory::Retention::new(writes, pool::raw::Physical::bind(key)),
                queue,
            },
        }
    }

    pub(in crate::network::listener) fn context<'step>(
        &'step mut self,
        work: app::Work<'step, 'd>,
    ) -> conn::Ctx<'e, 'step, 'p, 's, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>
    where
        'd: 'step,
    {
        let output = (!self.connection.is_sealed()).then_some(&self.output);
        conn::Ctx {
            slot: self.connection.reborrow(),
            output,
            work,
        }
    }
}
