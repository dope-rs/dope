use crate::Driver;
use crate::manifold::Outcome;
use crate::manifold::listener::{Aux, State};
use crate::transport::link::Slot;
use crate::transport::wire::{RecvChunk, Wire};

pub trait Application: Sized {
    type Conn: Default + 'static;
    type Wire: Wire;

    fn on_chunk<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        chunk: RecvChunk<'_>,
        aux: &mut Aux,
        driver: &'d Driver,
    ) -> Outcome;

    fn on_send<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        sent: usize,
        aux: &mut Aux,
        driver: &'d Driver,
    );

    fn on_close<'d>(&mut self, slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>, aux: &mut Aux);

    fn defer_close<'d>(&self, slot: &Slot<'d, Self::Wire, State<Self::Conn>>) -> bool {
        let _ = slot;
        false
    }

    /// A connection turned away by `per_ip_cap`; it never becomes a slot.
    fn on_capped(&mut self, peer_ip: std::net::IpAddr) {
        let _ = peer_ip;
    }

    fn on_wake<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
        driver: &'d Driver,
    ) {
        let _ = (slot, aux, driver);
    }

    fn on_accept<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
        driver: &'d Driver,
    ) -> Outcome {
        let _ = (slot, aux, driver);
        Outcome::Ok
    }
}
