use std::{marker, pin};

use dope_core::driver::{retained, schedule};
use dope_net::{link::pool, wire};

use crate::connector::{app, attempt, connection::engine::transfer::recv};

#[repr(transparent)]
pub(in crate::connector::connection::engine) struct Access<'a, H, S, E>(
    pin::Pin<&'a mut H>,
    marker::PhantomData<fn() -> (S, E)>,
);

impl<'a, H, S, E> Access<'a, H, S, E> {
    pub(in crate::connector::connection::engine) fn new(engine: pin::Pin<&'a mut H>) -> Self {
        Self(engine, marker::PhantomData)
    }
}

impl<'d, const ID: u8, A, S, E, H> app::Receiver<'d, ID, A> for Access<'_, H, S, E>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    H: recv::Recv<'d, ID, A, S, E>,
{
    fn borrowed(
        self,
        key: pool::Key<'d, ID>,
        batch: <A::Wire as wire::Wire>::RecvBatch<'_>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::BorrowedReceive<'d, ID>,
    {
        self.0.recv_batch(key, batch, turn, driver)
    }

    fn retained(
        self,
        key: pool::Key<'d, ID>,
        chunk: <A::Wire as wire::Wire>::RetainedRecv<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::RetainedReceive<'d, ID>,
    {
        self.0.recv_retained(key, chunk, turn, driver)
    }
}
