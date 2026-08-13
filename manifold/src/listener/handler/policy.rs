use std::{io, pin};

use dope_core::driver::{retained, schedule};
use dope_net::{link::pool, wire};

use crate::{
    listener,
    listener::handler,
    receive::{self, ingress},
};

pub trait Policy<'d, const ID: u8, A>: receive::Delivery + ingress::Dispatch
where
    A: handler::Application<'d, ID>,
{
    fn retained_capacity(max_connections: usize) -> io::Result<usize>;

    fn receive<E>(
        listener: pin::Pin<&mut listener::Listener<'d, ID, A, E>>,
        key: pool::Key<'d, ID>,
        input: <Self as receive::Delivery>::Value<'_, 'd, A::Wire>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        E: crate::Env<Wire = A::Wire>;
}

impl<'d, const ID: u8, A> Policy<'d, ID, A> for receive::Borrowed
where
    A: handler::BorrowedApplication<'d, ID>,
{
    fn retained_capacity(_: usize) -> io::Result<usize> {
        Ok(0)
    }

    fn receive<E>(
        mut listener: pin::Pin<&mut listener::Listener<'d, ID, A, E>>,
        key: pool::Key<'d, ID>,
        batch: <A::Wire as wire::Wire>::RecvBatch<'_>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        E: crate::Env<Wire = A::Wire>,
    {
        let mut this = listener.as_mut().project();
        if !this.schedule.inbound.arm(key, driver.turn_now()) {
            return crate::Outcome::Overrun;
        }
        let Some(mut egress) = this.owner.egress_mut(key) else {
            return crate::Outcome::Ok;
        };
        for chunk in batch {
            use dope_net::wire::RecvChunk;

            let outcome = match chunk {
                RecvChunk::Borrowed(chunk) => A::chunk(
                    this.app.as_mut(),
                    egress.context(turn.reborrow().application()),
                    chunk,
                    driver,
                ),
                RecvChunk::Owned(chunk) => A::chunk(
                    this.app.as_mut(),
                    egress.context(turn.reborrow().application()),
                    chunk,
                    driver,
                ),
            };
            if !matches!(outcome, crate::Outcome::Ok) {
                return outcome;
            }
        }
        crate::Outcome::Ok
    }
}

impl<'d, const ID: u8, A> Policy<'d, ID, A> for receive::Retained
where
    A: handler::RetainedApplication<'d, ID>,
{
    fn retained_capacity(max_connections: usize) -> io::Result<usize> {
        A::RETENTION.capacity(max_connections)
    }

    fn receive<E>(
        mut listener: pin::Pin<&mut listener::Listener<'d, ID, A, E>>,
        key: pool::Key<'d, ID>,
        chunk: <A::Wire as wire::Wire>::RetainedRecv<'d>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome
    where
        E: crate::Env<Wire = A::Wire>,
    {
        let mut this = listener.as_mut().project();
        if !this.schedule.inbound.arm(key, driver.turn_now()) {
            return crate::Outcome::Overrun;
        }
        match this.owner.egress_mut(key) {
            Some(mut egress) => A::retained_chunk(
                this.app.as_mut(),
                egress.context(turn.application()),
                chunk,
                driver,
            ),
            None => crate::Outcome::Ok,
        }
    }
}
