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

    fn on_chunk<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        chunk: RecvChunk<'_>,
        driver: &'d Driver,
    ) -> ChunkOutcome;

    fn on_connected<'d>(
        &mut self,
        tag: u32,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        driver: &'d Driver,
    );

    fn on_connect_failed(&mut self, tag: u32, driver: &Driver) {
        let _ = (tag, driver);
    }

    fn before_send<'d>(&mut self, slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>) {
        let _ = slot;
    }

    fn on_send<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        sent: usize,
        driver: &'d Driver,
    );

    fn on_close<'d>(&mut self, slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>);

    fn defer_close<'d>(&self, slot: &Slot<'d, Self::Wire, State<Self::Conn>>) -> bool {
        let _ = slot;
        false
    }

    fn is_drained<'d>(&self, slot: &Slot<'d, Self::Wire, State<Self::Conn>>) -> bool {
        let _ = slot;
        true
    }
}
