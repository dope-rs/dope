use dope_core::{driver::route, io::recv};

use crate::{
    link::{
        self, engine, pool,
        slot::{self, types},
    },
    wire::{self, batch, receive},
};

type Transaction<'a, 'd, const ID: u8, W> =
    <<W as wire::Wire>::Receive as receive::Strategy<W>>::Transaction<'a, 'd, ID>;
type Block<'a, 'd, const ID: u8, W> = receive::Block<'a, 'd, ID, W>;

pub(in crate::link) enum Reservation<'a, 'd, const ID: u8, W: wire::Wire> {
    Drop,
    Blocked(Blocked<'a, 'd, ID, W>),
    Ready(Prepared<'a, 'd, ID, W>),
}

pub(in crate::link) struct Blocked<'a, 'd, const ID: u8, W: wire::Wire> {
    pub(in crate::link) block: Block<'a, 'd, ID, W>,
    pub(in crate::link) credit: wire::RecvCredit<'d, ID>,
    pub(in crate::link) more: bool,
    pub(in crate::link) rearm: pool::Key<'d, ID>,
    pub(in crate::link) recv: &'a mut engine::Receive,
}

pub(in crate::link) struct Prepared<'a, 'd, const ID: u8, W: wire::Wire> {
    engine: &'a mut link::Engine<'d>,
    transaction: Transaction<'a, 'd, ID, W>,
    target: route::Operation<'d, route::KeyTag<ID>>,
    needs_rearm: bool,
}

pub(in crate::link) struct Reception<'a, 'd, const ID: u8, W: wire::Wire, S> {
    connection: &'a mut types::Connection<'d, ID, W, S>,
}

impl<'a, 'd, const ID: u8, W: wire::Wire, S> Reception<'a, 'd, ID, W, S> {
    pub(super) fn new(connection: &'a mut types::Connection<'d, ID, W, S>) -> Self {
        Self { connection }
    }

    pub(in crate::link) fn reserve<'recv>(
        self,
        runtime: &'recv mut W::RuntimeContext<'d, ID>,
        more: bool,
    ) -> Reservation<'recv, 'd, ID, W>
    where
        'a: 'recv,
    {
        let connection = self.connection;
        if !connection.engine.recv.is_armed() {
            return Reservation::Drop;
        }
        let rearm = connection.key();
        let target = rearm.target().dispatch();
        let ready = connection.engine.ready_handle();
        let transaction = match <W::Receive as receive::Strategy<W>>::reserve(
            &mut connection.wire,
            &mut connection.send,
            runtime,
        ) {
            Ok(transaction) => transaction,
            Err(block) => {
                let credit = wire::RecvCredit::new(ready, target);
                return Reservation::Blocked(Blocked {
                    block: receive::Block(block),
                    credit,
                    more,
                    rearm,
                    recv: &mut connection.engine.recv,
                });
            }
        };
        let closing = connection.engine.lifecycle.is_closing();
        let needs_rearm =
            connection
                .engine
                .recv
                .settle(&mut connection.engine.flights, more, closing);
        Reservation::Ready(Prepared {
            engine: &mut connection.engine,
            transaction,
            target,
            needs_rearm,
        })
    }

    pub(in crate::link) fn settle_closing_data(&mut self, more: bool) -> bool {
        let connection = &mut *self.connection;
        if !connection.engine.recv.is_armed() {
            return false;
        }
        connection
            .engine
            .recv
            .settle(&mut connection.engine.flights, more, true);
        !more
    }

    pub(in crate::link) fn eof<C>(&mut self, more: bool) -> slot::Decision<C> {
        let connection = &mut *self.connection;
        if !connection.engine.recv.is_armed() {
            return slot::Decision::Drop;
        }
        connection.engine.lifecycle.begin_close();
        connection
            .engine
            .recv
            .settle(&mut connection.engine.flights, more, true);
        W::recv_eof(&mut connection.wire);
        slot::Decision::Close
    }

    pub(in crate::link) fn cancelled<C>(&mut self, more: bool) -> slot::Decision<C> {
        let connection = &mut *self.connection;
        if !connection.engine.recv.is_armed() {
            return slot::Decision::Drop;
        }
        let closing = connection.engine.lifecycle.is_closing();
        let needs_rearm =
            connection
                .engine
                .recv
                .settle(&mut connection.engine.flights, more, closing);
        if !more && closing {
            slot::Decision::Close
        } else {
            slot::Decision::NoChunk { needs_rearm }
        }
    }

    pub(in crate::link) fn starved<C>(&mut self, more: bool) -> slot::Decision<C> {
        let connection = &mut *self.connection;
        if !connection.engine.recv.is_armed() {
            return slot::Decision::Drop;
        }
        let closing = connection.engine.lifecycle.is_closing();
        slot::Decision::NoChunk {
            needs_rearm: connection.engine.recv.settle(
                &mut connection.engine.flights,
                more,
                closing,
            ),
        }
    }

    pub(in crate::link) fn buffer_exhausted<C>(&mut self, more: bool) -> slot::Decision<C> {
        self.starved(more)
    }

    pub(in crate::link) fn failed<C>(&mut self, more: bool) -> slot::Decision<C> {
        let connection = &mut *self.connection;
        if !connection.engine.recv.is_armed() {
            return slot::Decision::Drop;
        }
        connection.engine.lifecycle.abort();
        connection
            .engine
            .recv
            .settle(&mut connection.engine.flights, more, true);
        slot::Decision::Close
    }
}

impl<'d, const ID: u8, W: wire::Wire> Prepared<'_, 'd, ID, W> {
    pub(in crate::link) fn data<'bytes>(
        mut self,
        slice: &'bytes mut [u8],
        capacity: &batch::Capacity<W>,
    ) -> slot::Decision<W::RecvBatch<'bytes>>
    where
        'd: 'bytes,
    {
        let swallowed = self.engine.discard.consume(slice.len());
        if swallowed == slice.len() {
            return slot::Decision::Discarded {
                needs_rearm: self.needs_rearm,
            };
        }
        let chunk =
            receive::Transaction::process(&mut self.transaction, &mut slice[swallowed..], capacity);
        let chunks = chunk.len();

        if chunks > capacity.items().get() {
            slot::Decision::Overrun {
                needs_rearm: self.needs_rearm,
            }
        } else if chunks == 0 {
            slot::Decision::NoChunk {
                needs_rearm: self.needs_rearm,
            }
        } else {
            slot::Decision::Chunk {
                chunk,
                needs_rearm: self.needs_rearm,
            }
        }
    }

    pub(in crate::link) fn retained<'bytes>(
        &mut self,
        mut bytes: recv::Lease<'bytes>,
    ) -> slot::Decision<W::RetainedRecv<'bytes>>
    where
        'd: 'bytes,
    {
        let swallowed = self.engine.discard.consume(bytes.as_slice().len());
        if swallowed == bytes.as_slice().len() {
            return slot::Decision::Discarded {
                needs_rearm: self.needs_rearm,
            };
        }
        bytes.advance(swallowed);
        match receive::Transaction::process_retained(&mut self.transaction, bytes) {
            Some(chunk) => slot::Decision::Chunk {
                chunk,
                needs_rearm: self.needs_rearm,
            },
            None => slot::Decision::NoChunk {
                needs_rearm: self.needs_rearm,
            },
        }
    }

    pub(in crate::link) fn bind_recv_credit(&mut self, chunk: &mut W::RetainedRecv<'d>) -> bool {
        let credit: wire::RecvCredit<'d, ID> =
            wire::RecvCredit::new(self.engine.ready_handle(), self.target);
        let binding = credit.binding();
        if !W::bind_recv_credit(chunk, credit).is_ok_and(|receipt| receipt.binds(binding)) {
            return false;
        }
        self.engine.recv.pause();
        self.engine.recv.needs_cancel()
    }
}
