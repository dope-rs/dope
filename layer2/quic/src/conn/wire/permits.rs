use o3::buffer::{storage, write};
use std::{mem, num};

use o3::alloc::VecExt as _;

use crate::conn::wire::packet;

pub(in crate::conn) trait Reservation: Sized {
    type Validated;

    /// The append owner exposes only its suffix and cannot shorten the retained prefix.
    fn out(&mut self) -> write::Append<'_>;
    fn limit(&self) -> usize;
    fn validate(self, overhead: usize) -> Result<Self::Validated, Self>;
    fn publish<T, U>(packet: Self::Validated, state: T, commit: impl FnOnce(T) -> U) -> U;
    fn rollback(self);
}

pub(in crate::conn) struct Batch<'sink> {
    sink: &'sink mut packet::Batch,
    start: usize,
    limit: usize,
}

/// An append reservation retaining the original owner's complete prefix.
pub(in crate::conn) struct Gso<'sink> {
    sink: &'sink mut packet::Gso,
    start: usize,
    limit: usize,
}

pub(in crate::conn) struct Fresh<'sink> {
    sink: &'sink mut Vec<Vec<u8>>,
    packet: Vec<u8>,
    limit: usize,
}

pub(in crate::conn) struct Single<'sink> {
    sink: &'sink mut Vec<u8>,
    limit: usize,
}

pub(in crate::conn) struct ValidBatch<'sink> {
    sink: &'sink mut packet::Batch,
    bytes: num::NonZero<u32>,
}

/// Retains the payload owner while its exclusive sink holds an allocation-free
/// vacant placeholder. Publication consumes that placeholder after commit.
pub(in crate::conn) struct ValidGso<'sink> {
    sink: &'sink mut packet::Gso,
    ready: packet::Ready,
}

pub(in crate::conn) struct ValidFresh<'sink> {
    sink: &'sink mut Vec<Vec<u8>>,
    packet: Vec<u8>,
}

pub(in crate::conn) struct ValidSingle<'sink> {
    _sink: &'sink mut Vec<u8>,
}

impl<'sink> Batch<'sink> {
    pub(super) fn new(sink: &'sink mut packet::Batch, limit: usize) -> Self {
        let start = sink.buf.len();
        Self { sink, start, limit }
    }
}

impl<'sink> Gso<'sink> {
    pub(super) fn new(sink: &'sink mut packet::Gso, limit: usize) -> Self {
        let start = sink.len();
        Self { sink, start, limit }
    }
}

impl<'sink> Fresh<'sink> {
    pub(super) fn new(
        sink: &'sink mut Vec<Vec<u8>>,
        limit: usize,
        overhead: usize,
    ) -> Result<Self, packet::Allocation> {
        let outer = packet::outer_limit(limit, overhead)?;
        let packet = Vec::try_new(outer).map_err(|error| packet::Allocation::Payload {
            error,
            requested: outer,
        })?;
        Ok(Self {
            sink,
            packet,
            limit,
        })
    }
}

impl<'sink> Single<'sink> {
    pub(super) fn new(
        sink: &'sink mut Vec<u8>,
        limit: usize,
        overhead: usize,
    ) -> Result<Self, packet::Allocation> {
        sink.clear();
        let outer = packet::outer_limit(limit, overhead)?;
        sink.try_grow(outer)
            .map_err(|error| packet::Allocation::Payload {
                error,
                requested: outer,
            })?;
        Ok(Self { sink, limit })
    }
}

impl<'sink> Reservation for Batch<'sink> {
    type Validated = ValidBatch<'sink>;

    fn out(&mut self) -> write::Append<'_> {
        write::Append::new(&mut self.sink.buf)
    }

    fn limit(&self) -> usize {
        self.limit
    }

    fn validate(self, overhead: usize) -> Result<Self::Validated, Self> {
        let bytes = self.sink.buf.len() - self.start;
        if bytes > overhead
            && bytes <= self.limit + overhead
            && let Ok(bytes) = u32::try_from(bytes)
            && let Some(bytes) = num::NonZero::<u32>::new(bytes)
        {
            return Ok(ValidBatch {
                sink: self.sink,
                bytes,
            });
        }
        Err(self)
    }

    fn publish<T, U>(packet: Self::Validated, state: T, commit: impl FnOnce(T) -> U) -> U {
        let output = commit(state);
        packet.sink.segs.push(packet.bytes.get());
        output
    }

    fn rollback(self) {
        self.sink.buf.truncate(self.start);
    }
}

impl<'sink> Reservation for Gso<'sink> {
    type Validated = ValidGso<'sink>;

    fn out(&mut self) -> write::Append<'_> {
        self.sink.out()
    }

    fn limit(&self) -> usize {
        self.limit
    }

    fn validate(self, overhead: usize) -> Result<Self::Validated, Self> {
        let bytes = self.sink.len() - self.start;
        let width = self
            .sink
            .segment_size()
            .or_else(|| u16::try_from(bytes).ok().and_then(num::NonZero::<u16>::new));
        let packets = num::NonZero::<u8>::MIN.checked_add(self.sink.packets() as u8);
        if bytes > overhead
            && bytes <= self.limit + overhead
            && let Some(width) = width
            && let Some(packets) = packets
        {
            let payload =
                match mem::replace(&mut self.sink.storage, packet::Storage::Vacant(Vec::new())) {
                    packet::Storage::Ready(ready) => ready.payload,
                    packet::Storage::Vacant(value) => match storage::NonEmpty::try_new(value) {
                        Ok(value) => value,
                        Err(empty) => {
                            self.sink.storage = packet::Storage::Vacant(empty.into_parts().0);
                            return Err(self);
                        }
                    },
                };
            return Ok(ValidGso {
                sink: self.sink,
                ready: packet::Ready {
                    payload,
                    packets,
                    segment_size: width,
                    sealed: bytes < usize::from(width.get()),
                },
            });
        }
        Err(self)
    }

    fn publish<T, U>(packet: Self::Validated, state: T, commit: impl FnOnce(T) -> U) -> U {
        let output = commit(state);
        mem::forget(mem::replace(
            &mut packet.sink.storage,
            packet::Storage::Ready(packet.ready),
        ));
        output
    }

    fn rollback(self) {
        match &mut self.sink.storage {
            packet::Storage::Vacant(value) => value.truncate(self.start),
            packet::Storage::Ready(ready) => {
                let start = num::NonZero::<usize>::MIN.saturating_add(self.start.saturating_sub(1));
                ready.payload.truncate(start);
            }
        }
    }
}

impl<'sink> Reservation for Fresh<'sink> {
    type Validated = ValidFresh<'sink>;

    fn out(&mut self) -> write::Append<'_> {
        write::Append::new(&mut self.packet)
    }

    fn limit(&self) -> usize {
        self.limit
    }

    fn validate(self, overhead: usize) -> Result<Self::Validated, Self> {
        if self.packet.len() > overhead && self.packet.len() <= self.limit + overhead {
            return Ok(ValidFresh {
                sink: self.sink,
                packet: self.packet,
            });
        }
        Err(self)
    }

    fn publish<T, U>(packet: Self::Validated, state: T, commit: impl FnOnce(T) -> U) -> U {
        let output = commit(state);
        packet.sink.push(packet.packet);
        output
    }

    fn rollback(self) {
        drop(self.packet);
    }
}

impl<'sink> Reservation for Single<'sink> {
    type Validated = ValidSingle<'sink>;

    fn out(&mut self) -> write::Append<'_> {
        write::Append::new(self.sink)
    }

    fn limit(&self) -> usize {
        self.limit
    }

    fn validate(self, overhead: usize) -> Result<Self::Validated, Self> {
        if self.sink.len() > overhead && self.sink.len() <= self.limit + overhead {
            return Ok(ValidSingle { _sink: self.sink });
        }
        Err(self)
    }

    fn publish<T, U>(_packet: Self::Validated, state: T, commit: impl FnOnce(T) -> U) -> U {
        commit(state)
    }

    fn rollback(self) {
        self.sink.clear();
    }
}
