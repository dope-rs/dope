use std::convert;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::{
    link::pool,
    wire::receive::{Chunk, batch},
};

use crate::{app::receive::ingress, client::app};

pub(in crate::client) trait Receiver<'w, 'd: 'w, ID: Identity, A>
where
    A: app::Application<'d, ID>,
{
    fn borrowed<'input, 'turn, 'driver, 'context, 'owner>(
        self,
        key: pool::Key<'d, ID>,
        batch: batch::Items<'input, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'input,
        'd: 'owner,
        A: app::Borrowed<'d, ID>;

    fn retained<'turn, 'driver, 'context, 'owner>(
        self,
        key: pool::Key<'d, ID>,
        chunk: Chunk<'w, 'd, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'owner,
        A: app::Owned<'w, 'd, ID>;
}
