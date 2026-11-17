use crate::proto::path::Peer;
use o3::buffer::{storage, write};
use std::{mem, num::NonZero};

mod flight;
pub(in crate::runtime) mod gate;
pub(in crate::runtime) mod quantum;
mod queue;
mod segments;

use crate::runtime::Transit;
pub use flight::Flight;
pub(in crate::runtime) use flight::Kind;
pub use queue::Queue;
pub use segments::Segments;

use o3::{
    alloc::{self, VecExt as _},
    depot::{capacity, queue::ring},
};

use crate::conn::{
    self,
    output::transmit,
    wire::{packet, permits},
};

pub(super) struct Storage {
    pub(super) pending: Pending,
    pub(super) bytes_capacity: usize,
    pub(super) cache: Cache,
}

/// Owns each queued flight and the exact sum of its immutable packet and byte extents.
pub(super) struct Pending {
    flights: ring::Fifo<Flight>,
    packets: usize,
    bytes: usize,
}

pub(super) struct Cache {
    pub(super) gso: Option<dope::core::io::datagram::GsoLimits>,
    pub(super) recycled: Vec<Vec<u8>>,
}

/// Vacant queue entry coupled to its packet and byte credits until commit.
pub(super) struct Reservation<'storage> {
    vacant: ring::Vacant<'storage, ring::Fifo<Flight>>,
    packets: &'storage mut usize,
    bytes: &'storage mut usize,
}

pub(super) struct Packet<'storage> {
    slot: Reservation<'storage>,
    cache: &'storage mut Cache,
    packet: Vec<u8>,
    peer: &'storage Peer,
    limit: usize,
}

pub(in crate::runtime) enum Submit<'quota, R, E> {
    Empty,
    Deferred,
    Accepted(R, quantum::Reservation<'quota>),
    Rejected(E),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::mux) enum Emission {
    Sent,
    Skipped,
    Allocation {
        error: alloc::Error,
        requested: usize,
    },
}

impl Pending {
    pub(super) fn capacity(&self) -> usize {
        self.flights.capacity()
    }

    pub(super) fn len(&self) -> usize {
        self.packets
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn is_empty(&self) -> bool {
        self.flights.is_empty()
    }

    fn reserve(&mut self, limit: usize, packet: usize, byte: usize) -> Option<Reservation<'_>> {
        let packet = NonZero::new(packet)?;
        let byte = NonZero::new(byte)?;
        if packet.get() > self.capacity() - self.packets
            || byte.get() > limit.saturating_sub(self.bytes)
        {
            return None;
        }
        let vacant = self.flights.vacant()?;
        Some(Reservation {
            vacant,
            packets: &mut self.packets,
            bytes: &mut self.bytes,
        })
    }

    fn pop_front(&mut self) -> Option<Flight> {
        let front = self.flights.front_entry()?;
        let packets = front.get().packet_count();
        let bytes = front.get().byte_len();
        let outgoing = front.remove();
        self.packets -= packets.get();
        self.bytes -= bytes;
        Some(outgoing)
    }

    fn submit<'quota, R, E>(
        &mut self,
        credit: quantum::Credit<'quota>,
        submit: impl for<'op> FnOnce(Transit<'op>) -> Result<R, (Transit<'op>, E)>,
    ) -> Submit<'quota, R, E> {
        let Some(front) = self.flights.front_entry() else {
            return Submit::Empty;
        };
        let count = front.get().packet_count();
        let Some(grant) = credit.try_reserve(count) else {
            return Submit::Deferred;
        };
        let len = front.get().byte_len();
        let packets = self.packets - count.get();
        let bytes = self.bytes - len;
        let (outgoing, vacant) = front.take();
        match Transit::scope(outgoing, submit) {
            Ok(value) => {
                self.packets = packets;
                self.bytes = bytes;
                Submit::Accepted(value, grant)
            }
            Err((outgoing, error)) => {
                vacant.push_front(outgoing);
                Submit::Rejected(error)
            }
        }
    }
}

impl Reservation<'_> {
    // Private producers construct flights within their admitted packet and byte extents.
    #[inline(always)]
    fn commit(self, outgoing: Flight) -> NonZero<usize> {
        let count = outgoing.packet_count();
        let len = outgoing.byte_len();
        let packets = *self.packets + count.get();
        let bytes = *self.bytes + len;
        self.vacant.push_back(outgoing);
        *self.packets = packets;
        *self.bytes = bytes;
        count
    }
}

impl Cache {
    fn recycle(&mut self, mut packet: Vec<u8>) {
        packet.clear();
        if self.recycled.len() < self.recycled.capacity() {
            self.recycled.push(packet);
        }
    }
}

impl Storage {
    pub(super) fn try_new(capacity: capacity::Count, limit: usize) -> Result<Self, alloc::Error> {
        Ok(Self {
            pending: Pending {
                flights: ring::Fifo::try_new(capacity)?,
                packets: 0,
                bytes: 0,
            },
            bytes_capacity: limit,
            cache: Cache {
                gso: None,
                recycled: Vec::try_new(capacity.get())?,
            },
        })
    }

    /// Removes one flight and returns its packet and byte credits atomically.
    pub(super) fn pop_front(&mut self) -> Option<Flight> {
        self.pending.pop_front()
    }

    pub(super) fn gso_room(&self, max: NonZero<usize>) -> usize {
        let Some(limits) = self.cache.gso else {
            return 0;
        };
        let count = limits
            .capacity()
            .count()
            .min(conn::MAX_BATCH_PACKETS)
            .min(self.pending.capacity() - self.pending.len())
            .min(self.bytes_capacity.saturating_sub(self.pending.bytes()) / max);
        crate::runtime::raw::Gso::capacity::<true>(limits, self.bytes_capacity, max, count)
    }

    pub(super) fn emit_gso<E>(
        &mut self,
        packet: usize,
        byte: usize,
        outer: usize,
        participants: usize,
        fallback: Peer,
        emit: impl FnOnce(&mut crate::runtime::raw::Gso) -> (Option<Peer>, Result<(), E>),
    ) -> Result<(usize, Result<(), E>), conn::output::transmit::Admission> {
        let Self {
            pending,
            bytes_capacity: limit,
            cache,
        } = self;
        let Some(slot) = pending.reserve(*limit, packet, byte) else {
            return Ok((0, Ok(())));
        };
        let Some(limits) = cache.gso else {
            return Ok((0, Ok(())));
        };
        let bytes = cache.recycled.pop().unwrap_or_default();
        let mut batch =
            crate::runtime::raw::Gso::try_new(limits, bytes, packet, outer, participants)?;
        let (addr, result) = emit(&mut batch);
        let ready = match batch.finish() {
            packet::Storage::Ready(ready) => ready,
            packet::Storage::Vacant(bytes) => {
                cache.recycle(bytes);
                return Ok((0, result));
            }
        };
        let addr = addr.unwrap_or(fallback);
        let outgoing = if ready.packets.get() == 1 {
            Flight::plain(addr, ready.payload)
        } else {
            Flight::batch(addr, ready.payload, ready.segment_size, ready.packets)
        };
        Ok((slot.commit(outgoing).get(), result))
    }

    pub(super) fn emit_owned(
        &mut self,
        byte: usize,
        required: usize,
        build: impl FnOnce(Vec<u8>) -> Result<Flight, (Vec<u8>, Option<alloc::Error>)>,
    ) -> Emission {
        let Self {
            pending,
            bytes_capacity: limit,
            cache,
        } = self;
        let Some(slot) = pending.reserve(*limit, 1, byte) else {
            return Emission::Skipped;
        };
        let mut packet = cache.recycled.pop().unwrap_or_default();
        if let Err(error) = packet.try_grow(required) {
            cache.recycle(packet);
            return Emission::Allocation {
                error,
                requested: required,
            };
        }
        match build(packet) {
            Ok(outgoing) => {
                slot.commit(outgoing);
                Emission::Sent
            }
            Err((packet, error)) => {
                cache.recycle(packet);
                match error {
                    Some(error) => Emission::Allocation {
                        error,
                        requested: required,
                    },
                    None => Emission::Skipped,
                }
            }
        }
    }
}

impl packet::Sink for Storage {
    type Reservation<'sink> = Packet<'sink>;

    #[inline(always)]
    fn reserve<'sink>(
        &'sink mut self,
        limit: usize,
        destination: &'sink transmit::Destination,
    ) -> Result<Option<Self::Reservation<'sink>>, packet::Allocation> {
        let transmit::Destination::Network(peer) = destination else {
            return Ok(None);
        };
        let outer = packet::outer_limit(limit, destination.overhead())?;
        let Self {
            pending,
            bytes_capacity: cap,
            cache,
        } = self;
        let Some(slot) = pending.reserve(*cap, 1, outer) else {
            return Ok(None);
        };
        let mut packet = cache.recycled.pop().unwrap_or_default();
        if let Err(error) = packet.try_grow(outer) {
            cache.recycle(packet);
            return Err(packet::Allocation::Payload {
                error,
                requested: outer,
            });
        }
        Ok(Some(Packet {
            slot,
            cache,
            packet,
            peer,
            limit,
        }))
    }
}

impl<'storage> permits::Reservation for Packet<'storage> {
    type Validated = (Reservation<'storage>, Flight);

    fn out(&mut self) -> write::Append<'_> {
        write::Append::new(&mut self.packet)
    }

    fn limit(&self) -> usize {
        self.limit
    }

    #[inline(always)]
    fn validate(mut self, overhead: usize) -> Result<Self::Validated, Self> {
        if self.packet.len() > overhead && self.packet.len() <= self.limit + overhead {
            match storage::NonEmpty::try_new(mem::take(&mut self.packet)) {
                Ok(packet) => return Ok((self.slot, Flight::plain(*self.peer, packet))),
                Err(empty) => self.packet = empty.into_parts().0,
            }
        }
        Err(self)
    }

    #[inline(always)]
    fn publish((slot, packet): Self::Validated) {
        slot.commit(packet);
    }

    #[inline(always)]
    fn rollback(self) {
        self.cache.recycle(self.packet);
    }
}
