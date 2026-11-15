use std::{marker, net};

use dope_core::io::recv;
use o3::{
    alloc,
    base::marker::ThreadBound,
    depot::{capacity, queue::ring},
    mem::Rejected,
};

use super::Packet;

/// Bounded custody of original native datagrams awaiting dispatch.
pub struct Backlog<'d> {
    queue: ring::Fifo<(net::SocketAddr, recv::Lease<'d>)>,
    thread: marker::PhantomData<ThreadBound>,
}

impl<'d> Backlog<'d> {
    pub fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            queue: ring::Fifo::try_new(capacity)?,
            thread: marker::PhantomData,
        })
    }

    pub fn try_push<'turn>(
        &mut self,
        source: net::SocketAddr,
        packet: Packet<'turn, 'd>,
    ) -> Result<(), Rejected<Packet<'turn, 'd>, ring::Full>> {
        let vacant = match self.queue.try_vacant() {
            Ok(vacant) => vacant,
            Err(cause) => return Err(Rejected::new(packet, cause)),
        };
        vacant.push_back((source, packet.into_lease()));
        Ok(())
    }

    pub fn pop_front(&mut self) -> Option<(net::SocketAddr, Packet<'_, 'd>)> {
        let (source, lease) = self.queue.pop_front()?;
        Some((source, Packet::new(lease)))
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn clear(&mut self) {
        self.queue.clear();
    }
}
