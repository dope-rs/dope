use super::session::State;
use crate::Driver;
use crate::transport::link::Slot;
use crate::transport::wire::{RecvChunk, Wire};

pub enum ChunkOutcome {
    Ok,
    Overrun,
    CloseReconnect,
    ClosePermanent,
}

pub trait ConnApp: Sized {
    type Conn: Default;
    type Wire: Wire;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        chunk: RecvChunk<'_>,
        driver: &mut Driver,
    ) -> ChunkOutcome;

    fn on_connected(
        &mut self,
        tag: u32,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        driver: &mut Driver,
    );

    fn on_connect_failed(&mut self, tag: u32, driver: &mut Driver) {
        let _ = (tag, driver);
    }

    fn before_send(&mut self, slot: &mut Slot<Self::Wire, State<Self::Conn>>) {
        let _ = slot;
    }

    fn on_send(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        sent: usize,
        driver: &mut Driver,
    );

    fn on_close(&mut self, slot: &mut Slot<Self::Wire, State<Self::Conn>>);

    fn defer_close(&self, slot: &Slot<Self::Wire, State<Self::Conn>>) -> bool {
        let _ = slot;
        false
    }

    fn is_drained(&self, slot: &Slot<Self::Wire, State<Self::Conn>>) -> bool {
        let _ = slot;
        true
    }
}
