use std::convert;

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::{
    link::pool,
    wire::receive::{Chunk, batch},
};

use crate::{app::receive::ingress, client::app};

pub(in crate::client) trait Receiver<'w, 'd: 'w, ID: Identity, A>
where
    A: app::Application<'d, ID>,
{
    fn borrowed<'input, 'turn, 'driver, 'context>(
        self,
        key: pool::Key<'d, ID>,
        batch: batch::Items<'input, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'input,
        A: app::Borrowed<'d, ID>;

    fn retained<'turn, 'driver, 'context>(
        self,
        key: pool::Key<'d, ID>,
        chunk: Chunk<'w, 'd, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        A: app::Owned<'w, 'd, ID>;
}
