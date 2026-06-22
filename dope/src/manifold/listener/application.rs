use crate::Driver;
use crate::manifold::Outcome;
use crate::manifold::listener::{Aux, State};
use crate::transport::link::Slot;
use crate::transport::wire::{RecvChunk, Wire};

pub trait Application: Sized {
    type Conn: Default + 'static;
    type Wire: Wire;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        chunk: RecvChunk<'_>,
        aux: &mut Aux,
        driver: &mut Driver,
    ) -> Outcome;

    fn on_send(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        sent: usize,
        aux: &mut Aux,
        driver: &mut Driver,
    );

    fn on_close(&mut self, slot: &mut Slot<Self::Wire, State<Self::Conn>>, aux: &mut Aux);

    fn defer_close(&self, slot: &Slot<Self::Wire, State<Self::Conn>>) -> bool {
        let _ = slot;
        false
    }

    fn on_wake(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
        driver: &mut Driver,
    ) {
        let _ = (slot, aux, driver);
    }

    fn on_accept(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
        driver: &mut Driver,
    ) -> Outcome {
        let _ = (slot, aux, driver);
        Outcome::Ok
    }
}
