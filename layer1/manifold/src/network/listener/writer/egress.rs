use dope_core::driver::{route::Identity, schedule::app};
use dope_net::link::{egress, pool::pending};

use crate::network::listener::{
    self, conn, handler,
    writer::{self, deadline::Slot, memory},
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
    pub(in crate::network::listener) flights: &'a crate::Flights<'d, ID>,
    pub(in crate::network::listener) connection:
        Slot<'a, 'w, 'd, ID, A::Wire, writer::Record<'p, 's, 'd, ID, A>>,
    pub(in crate::network::listener) pending: pending::Handle<'a, 'd, ID>,
    pub(in crate::network::listener) queue: egress::Queue<
        'a,
        'e,
        'd,
        { listener::IOV_CAP },
        writer::Payload<'p, 's, 'd, ID, A::Egress>,
        writer::Reserve<'d, A::Egress>,
    >,
    pub(in crate::network::listener::writer) retention:
        memory::Retention<'p, 's, 'd, ID, A::Egress>,
}

impl<'e, 'a, 'p, 's, 'w, 'd: 'w, ID: Identity, A> Egress<'e, 'a, 'p, 's, 'w, 'd, ID, A>
where
    A: handler::Application<'d, ID>,
{
    pub(in crate::network::listener) fn context<'step>(
        &'step mut self,
        work: app::Work<'step, 'd>,
    ) -> conn::Ctx<'e, 'step, 'p, 's, 'w, 'd, ID, A::Wire, A::Conn, A::Egress>
    where
        'd: 'step,
    {
        conn::Ctx {
            slot: self.connection.reborrow(),
            output: conn::Output::Open {
                slots: self.flights,
                permit: self.pending,
                held: self.retention.reborrow(),
                queue: self.queue.reborrow(),
            },
            work,
        }
    }
}
