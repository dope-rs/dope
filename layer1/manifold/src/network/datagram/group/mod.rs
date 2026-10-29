//! Fixed, exclusively owned UDP sockets sharing one bounded send queue.

use std::{net, pin};

use dope_core::{
    driver::route::{Identity, indices::SlotIndex},
    io::{datagram::ReceiveError, fd::handles},
};
use o3::{buffer::pool, depot::queue::indexed::fifo};

use crate::{
    app::dispatch,
    network::datagram::{
        RejectCause, Rejected, packet, send,
        socket::{receive::Receive, sender::Sender},
    },
};

pub mod plan;
pub mod raw;

/// One packet retains its receiving socket identity and dispatch-turn borrow.
pub trait Handler<'d, ID: Identity> {
    fn packet<'turn>(
        self: pin::Pin<&mut Self>,
        received: Result<(net::SocketAddr, packet::Packet<'turn, 'd>), ReceiveError>,
        source: SlotIndex,
        sockets: Access<'turn, 'd, ID>,
    );

    /// Observes a native completion or submission failure after releasing its payload.
    fn sent(self: pin::Pin<&mut Self>, outcome: send::Outcome);
}

pub(super) struct Member<'d, ID: Identity> {
    pub(super) receive: Receive<'d, ID>,
    pub(super) fd: handles::Datagram<'d>,
    pub(super) addr: net::SocketAddr,
}

/// Read-only handler access retaining the installed group and driver brand.
#[repr(transparent)]
pub struct View<'step, 'd: 'step, ID: Identity, H> {
    owner: dispatch::raw::Installed<'step, 'd, raw::Endpoint<'d, ID, H>>,
}

impl<'step, 'd: 'step, ID: Identity, H> View<'step, 'd, ID, H> {
    pub fn handler(&self) -> &H {
        self.owner.as_ref().get_ref().handler()
    }
}

/// Exclusive queue admission against this group's immutable socket domain.
pub struct Access<'turn, 'd, ID: Identity> {
    sender: &'turn mut Sender<'d, ID, pool::Cursor, send::Routed>,
    ready: &'turn mut fifo::List<SlotIndex>,
    members: &'turn [Member<'d, ID>],
}

impl<'d, ID: Identity> Access<'_, 'd, ID> {
    pub fn queue_packet<'turn>(
        &mut self,
        packet: packet::Packet<'turn, 'd>,
        slot: SlotIndex,
        address: net::SocketAddr,
    ) -> Result<(), Rejected<packet::Packet<'turn, 'd>>> {
        let Some(member) = self.members.get(slot.index().get() as usize) else {
            return Err(Rejected::new(
                packet,
                RejectCause::Slot {
                    requested: slot,
                    capacity: self.members.len(),
                },
            ));
        };
        self.sender
            .try_receive(packet.into_lease(), address, slot)
            .map_err(|rejected| rejected.map(packet::Packet::new))?;
        if !member.receive.is_live() {
            mark(self.ready, slot);
        }
        Ok(())
    }
}

fn mark(ready: &mut fifo::List<SlotIndex>, slot: SlotIndex) {
    if let Some(entry) = ready.vacant_entry(slot.index().get()) {
        entry.push_back(slot);
    }
}
