use std::pin;

use dope_core::driver::{retained, schedule};
use dope_net::{link::pool, wire};
use o3::buffer::bytes;
use resume::Resume as _;

use crate::connector::{
    app, attempt, auxiliary,
    connection::{self, engine::transfer::resume},
};

pub(in crate::connector) trait Recv<'d, const ID: u8, A, S, E>: Sized
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn recv_batch(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        batch: <A::Wire as wire::Wire>::RecvBatch<'_>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::BorrowedReceive<'d, ID>;
    fn recv_retained(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        chunk: <A::Wire as wire::Wire>::RetainedRecv<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::RetainedReceive<'d, ID>;
    fn recv_chunk<R: bytes::Retainable>(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        chunk: R,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::BorrowedReceive<'d, ID>;
    fn recv_retained_chunk(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        chunk: <A::Wire as wire::Wire>::RetainedRecv<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::RetainedReceive<'d, ID>;
}

impl<'d, const ID: u8, A, S, E, X> Recv<'d, ID, A, S, E> for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn recv_batch(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        batch: <A::Wire as wire::Wire>::RecvBatch<'_>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::BorrowedReceive<'d, ID>,
    {
        if let Some(slot) = self.as_mut().project().pool.get_mut(key) {
            slot.state.last_recv = Some(driver.turn_now());
        }
        for chunk in batch {
            use dope_net::wire::RecvChunk;

            let outcome = match chunk {
                RecvChunk::Borrowed(chunk) => {
                    self.as_mut()
                        .recv_chunk(key, chunk, turn.reborrow(), driver)
                }
                RecvChunk::Owned(chunk) => {
                    self.as_mut()
                        .recv_chunk(key, chunk, turn.reborrow(), driver)
                }
            };
            if !matches!(outcome, crate::Outcome::Ok) {
                return outcome;
            }
        }
        crate::Outcome::Ok
    }

    fn recv_retained(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        chunk: <A::Wire as wire::Wire>::RetainedRecv<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::RetainedReceive<'d, ID>,
    {
        if let Some(slot) = self.as_mut().project().pool.get_mut(key) {
            slot.state.last_recv = Some(driver.turn_now());
        }
        self.recv_retained_chunk(key, chunk, turn, driver)
    }

    fn recv_chunk<R: bytes::Retainable>(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        chunk: R,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::BorrowedReceive<'d, ID>,
    {
        let outcome = {
            let this = self.as_mut().project();
            let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                queue: egress,
            }) = this.pool.egress_mut(key)
            else {
                return crate::Outcome::Ok;
            };
            if this.auxiliary.settle(
                &mut slot.state.owner,
                Err(auxiliary::Error::Transport),
                driver.region_token(),
            ) {
                return crate::Outcome::CloseAfter;
            }
            this.app.chunk(
                connection::Ctx::new(slot, turn.reborrow().application()),
                egress,
                chunk,
                driver,
            )
        };
        self.finish_chunk(key, outcome, turn, driver)
    }

    fn recv_retained_chunk(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        chunk: <A::Wire as wire::Wire>::RetainedRecv<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        A: app::RetainedReceive<'d, ID>,
    {
        let outcome = {
            let this = self.as_mut().project();
            let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                queue: egress,
            }) = this.pool.egress_mut(key)
            else {
                return crate::Outcome::Ok;
            };
            if this.auxiliary.settle(
                &mut slot.state.owner,
                Err(auxiliary::Error::Transport),
                driver.region_token(),
            ) {
                return crate::Outcome::CloseAfter;
            }
            this.app.retained_chunk(
                connection::Ctx::new(slot, turn.reborrow().application()),
                egress,
                chunk,
                driver,
            )
        };
        self.finish_chunk(key, outcome, turn, driver)
    }
}
