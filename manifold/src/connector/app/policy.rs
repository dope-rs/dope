use std::io;

use dope_core::driver::{retained, schedule};
use dope_net::{link::pool, wire};

use crate::{
    connector::app,
    receive::{self, ingress},
};

pub trait Policy<'d, const ID: u8, A>: receive::Delivery + ingress::Dispatch
where
    A: app::Application<'d, ID>,
{
    fn retained_capacity(max_connections: usize) -> io::Result<usize>;

    fn receive<R>(
        receiver: R,
        key: pool::Key<'d, ID>,
        input: <Self as receive::Delivery>::Value<'_, 'd, A::Wire>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        R: app::Receiver<'d, ID, A>;
}

impl<'d, const ID: u8, A> Policy<'d, ID, A> for receive::Borrowed
where
    A: app::BorrowedReceive<'d, ID>,
{
    fn retained_capacity(_: usize) -> io::Result<usize> {
        Ok(0)
    }

    fn receive<R>(
        receiver: R,
        key: pool::Key<'d, ID>,
        batch: <A::Wire as wire::Wire>::RecvBatch<'_>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        R: app::Receiver<'d, ID, A>,
    {
        receiver.borrowed(key, batch, turn, driver)
    }
}

impl<'d, const ID: u8, A> Policy<'d, ID, A> for receive::Retained
where
    A: app::RetainedReceive<'d, ID>,
{
    fn retained_capacity(max_connections: usize) -> io::Result<usize> {
        A::RETENTION.capacity(max_connections)
    }

    fn receive<R>(
        receiver: R,
        key: pool::Key<'d, ID>,
        chunk: <A::Wire as wire::Wire>::RetainedRecv<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        R: app::Receiver<'d, ID, A>,
    {
        receiver.retained(key, chunk, turn, driver)
    }
}
