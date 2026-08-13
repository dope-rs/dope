use std::ops;

use crate::{
    link::{
        self, egress, pool,
        slot::{self, reception, send},
    },
    wire,
};

pub(in crate::link) struct Stored<'d, const ID: u8, W: wire::Wire, S, const IOV: usize> {
    pub(in crate::link) connection: Connection<'d, ID, W, S>,
    pub(in crate::link) egress: egress::Lane<'d, IOV>,
}

pub struct Connection<'d, const ID: u8, W: wire::Wire, S> {
    pub(in crate::link) engine: link::Engine<'d>,
    pub(in crate::link) wire: W::Connection<'d, ID>,
    pub(in crate::link) send: W::StorageBackend<'d>,
    pub state: S,
    key: pool::Key<'d, ID>,
}

impl<'d, const ID: u8, W: wire::Wire, S> Connection<'d, ID, W, S> {
    pub(in crate::link) fn new(
        engine: link::Engine<'d>,
        wire: W::Connection<'d, ID>,
        send: W::StorageBackend<'d>,
        key: pool::Key<'d, ID>,
        state: S,
    ) -> Self {
        Self {
            engine,
            wire,
            send,
            state,
            key,
        }
    }

    pub fn key(&self) -> pool::Key<'d, ID> {
        self.key
    }

    pub fn send_status(&self) -> send::Status<'_, 'd, ID, W, S> {
        send::Status::new(self)
    }

    pub fn is_closing(&self) -> bool {
        self.engine.lifecycle.is_closing()
    }

    pub fn is_established(&self) -> bool {
        self.engine.establish.is_done()
    }

    pub fn begin_close(&mut self) {
        self.engine.lifecycle.begin_close();
    }

    pub fn should_close(&self, defer: bool) -> bool {
        self.engine
            .lifecycle
            .should_close(self.engine.sending.is_inflight(), defer)
    }

    pub fn abort(&mut self) {
        self.engine.lifecycle.abort();
    }

    pub fn is_aborted(&self) -> bool {
        self.engine.lifecycle.is_aborted()
    }

    pub fn set_close_after(&mut self) {
        self.engine.lifecycle.set_close_after();
    }

    pub fn close_after(&self) -> bool {
        self.engine.lifecycle.close_after()
    }

    pub fn begin_discard(&mut self, bytes: usize) -> bool {
        if bytes == 0
            || !W::RAW_RECV
            || self.engine.discard.remaining() > 0
            || self.engine.lifecycle.is_closing()
            || self.engine.lifecycle.close_after()
        {
            return false;
        }
        self.engine.discard.begin(bytes);
        true
    }

    pub fn io(&self) -> slot::Io<'_, 'd> {
        slot::Io::new(&self.engine)
    }

    pub fn sending(&mut self) -> send::Sending<'_, 'd, ID, W, S> {
        send::Sending::new(self)
    }

    #[doc(hidden)]
    pub fn split_direct_sending(&mut self) -> (&mut S, send::DirectSending<'_, 'd, ID, W>) {
        let target = self.key.target();
        let Self {
            engine,
            wire,
            send,
            state,
            ..
        } = self;
        (state, send::DirectSending::new(engine, wire, send, target))
    }

    pub(in crate::link) fn receiving(&mut self) -> reception::Reception<'_, 'd, ID, W, S> {
        reception::Reception::new(self)
    }
}

impl<'d, const ID: u8, W: wire::Wire, S, const IOV: usize> Stored<'d, ID, W, S, IOV> {
    pub(in crate::link) fn new(
        connection: Connection<'d, ID, W, S>,
        egress: egress::Lane<'d, IOV>,
    ) -> Self {
        Self { connection, egress }
    }

    pub(in crate::link) fn into_connection(self) -> Connection<'d, ID, W, S> {
        self.connection
    }
}

impl<'d, const ID: u8, W: wire::Wire, S, const IOV: usize> ops::Deref
    for Stored<'d, ID, W, S, IOV>
{
    type Target = Connection<'d, ID, W, S>;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<const ID: u8, W: wire::Wire, S, const IOV: usize> ops::DerefMut for Stored<'_, ID, W, S, IOV> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}
