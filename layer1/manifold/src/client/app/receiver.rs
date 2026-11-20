use dope_core::driver::{self, route::Identity, schedule};
use dope_net::{
    link::pool::recv::input,
    wire::receive::{Chunk, batch},
};

use crate::{app::receive::Outcome, client::app};

pub(in crate::client) trait Receiver<'w, 'd: 'w, ID: Identity, A>
where
    A: app::Application<'d, ID>,
{
    type Remaining;

    fn borrowed<'input, 'turn, 'driver, 'context>(
        self,
        batch: batch::Items<'input, A::Wire>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> (Self::Remaining, Outcome)
    where
        'd: 'input,
        A: app::Borrowed<'d, ID>;

    fn retained<'turn, 'driver, 'context, R: Default>(
        self,
        chunk: Chunk<'w, 'd, A::Wire>,
        receipt: input::Receipt<A::Wire, crate::app::receive::Owned<R>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> (Self::Remaining, Outcome)
    where
        A: app::Owned<'w, 'd, ID, R>;
}
