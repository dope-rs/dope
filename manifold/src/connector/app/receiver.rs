use dope_core::driver::{retained, schedule};
use dope_net::{link::pool, wire};

use crate::connector::app;

pub trait Receiver<'d, const ID: u8, A>
where
    A: app::Application<'d, ID>,
{
    fn borrowed(
        self,
        key: pool::Key<'d, ID>,
        batch: <A::Wire as wire::Wire>::RecvBatch<'_>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::BorrowedReceive<'d, ID>;

    fn retained(
        self,
        key: pool::Key<'d, ID>,
        chunk: <A::Wire as wire::Wire>::RetainedRecv<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::RetainedReceive<'d, ID>;
}
