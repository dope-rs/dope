use std::{convert::Infallible, marker};

use o3::base::cell::region;

use crate::wire::packet::{
    self, Bound, Name, Offset, Packet, Part, Read, Record, Records, error, records,
};

#[derive(Clone, Copy)]
pub(in crate::wire) struct Span<'id> {
    pub(super) start: Offset,
    pub(super) end: Offset,
    packet: region::Proof<region::Identity<'id>>,
}

#[derive(Clone, Copy)]
pub(in crate::wire) struct Checked;

/// Replay mode issued only after the complete immutable traversal succeeds.
#[derive(Clone, Copy)]
pub(in crate::wire) struct Validated;

pub(in crate::wire) trait Mode: Copy {
    type Error;

    fn check(test: impl FnOnce() -> Result<(), error::Packet>) -> Result<(), Self::Error>;

    /// Returns exactly `length` initialized packet bytes and their ending offset.
    ///
    /// # Safety
    /// Validated mode requires the exact range's immutable traversal proof.
    unsafe fn read(
        packet: Packet<'_>,
        position: Offset,
        length: u16,
    ) -> Result<Read<'_>, Self::Error>;

    /// # Safety
    /// Validated mode requires the exact range's immutable traversal proof.
    unsafe fn array<const N: usize>(
        packet: Packet<'_>,
        position: Offset,
    ) -> Result<([u8; N], Offset), Self::Error> {
        const { assert!(N <= u16::MAX as usize) };
        // SAFETY: checked mode admits N bytes; validated mode retains that range.
        let read = unsafe { Self::read(packet, position, N as u16)? };
        // SAFETY: both modes return exactly N initialized bytes in this packet.
        Ok((unsafe { *read.bytes.as_ptr().cast::<[u8; N]>() }, read.end))
    }
}

impl Mode for Checked {
    type Error = error::Packet;

    fn check(test: impl FnOnce() -> Result<(), error::Packet>) -> Result<(), Self::Error> {
        test()
    }

    unsafe fn read(
        packet: Packet<'_>,
        position: Offset,
        length: u16,
    ) -> Result<Read<'_>, Self::Error> {
        packet::bytes(packet, position, length)
    }
}

impl Mode for Validated {
    type Error = Infallible;

    fn check(_: impl FnOnce() -> Result<(), error::Packet>) -> Result<(), Self::Error> {
        Ok(())
    }

    unsafe fn read(
        packet: Packet<'_>,
        position: Offset,
        length: u16,
    ) -> Result<Read<'_>, Self::Error> {
        let start = position.get();
        let end = start + usize::from(length);
        Ok(Read {
            // SAFETY: the complete immutable traversal checked this exact range.
            bytes: unsafe { packet.bytes.get_unchecked(start..end) },
            end: Offset::new(end as u16),
        })
    }
}

impl<'id> Span<'id> {
    pub(in crate::wire) fn slice<'packet>(self, packet: Bound<'packet, 'id>) -> &'packet [u8] {
        let Span {
            start,
            end,
            packet: _proof,
        } = self;
        // SAFETY: record admission checked this range in the same branded,
        // immutable packet; the retained Span preserves its ordered endpoints.
        packet::validated(unsafe {
            Validated::read(packet.packet, start, end.raw().wrapping_sub(start.raw()))
        })
        .bytes
    }
}

pub(super) struct Label<'packet> {
    pub(super) packet: Packet<'packet>,
    pub(super) position: Offset,
}

impl<'packet> Label<'packet> {
    pub(super) fn try_read(self) -> Result<Part<'packet>, error::Packet> {
        // SAFETY: checked mode validates every accessed range and tag.
        unsafe { self.decode::<Checked>() }
    }

    /// # Safety
    /// The position must belong to a completely validated immutable name traversal.
    pub(super) unsafe fn read(self) -> Part<'packet> {
        // SAFETY: the caller retains the complete traversal proof for this packet.
        packet::validated(unsafe { self.decode::<Validated>() })
    }

    /// Validated mode requires a position reached by the immutable traversal.
    unsafe fn decode<M: Mode>(self) -> Result<Part<'packet>, M::Error> {
        let Self { packet, position } = self;
        // SAFETY: checked mode validates; otherwise the traversal proves this header.
        let ([length], end) = unsafe { M::array::<1>(packet, position)? };
        if length & 0xc0 == 0xc0 {
            // SAFETY: the same traversal proves both compression-pointer bytes.
            let ([_, low], end) = unsafe { M::array::<2>(packet, position)? };
            let target = Offset::new(u16::from_be_bytes([length & 0x3f, low]));
            M::check(|| {
                if target.get() >= packet.bytes.len() || target >= position {
                    return Err(error::Packet::Pointer {
                        offset: position.get(),
                        target: target.get(),
                        length: packet.bytes.len(),
                    });
                }
                Ok(())
            })?;
            return Ok(Part::Pointer { target, end });
        }
        M::check(|| {
            if length & 0xc0 != 0 {
                return Err(error::Packet::Label {
                    offset: position.get(),
                    value: length,
                });
            }
            Ok(())
        })?;
        // SAFETY: validation establishes this label's entire immutable range.
        Ok(Part::Label(unsafe {
            M::read(packet, end, u16::from(length))?
        }))
    }
}

impl<'packet, 'id, M: Mode> Cursor<'packet, 'id, M> {
    pub(in crate::wire) const fn position(self) -> Offset {
        self.position
    }

    pub(in crate::wire) fn name(&mut self) -> Result<Name<'id>, M::Error> {
        let offset = self.position;
        let mut position = self.position;
        let mut prefix = 0usize;
        loop {
            // SAFETY: unchecked cursors are retained only by a validated Records traversal.
            let part = unsafe {
                Label {
                    packet: self.packet.packet,
                    position,
                }
                .decode::<M>()?
            };
            position = part.end();
            let Part::Label(label) = part else {
                break;
            };
            if label.bytes.is_empty() {
                break;
            }
            prefix += label.bytes.len() + 1;
            M::check(|| {
                if prefix + 1 > crate::host::Name::MAX_WIRE_LEN {
                    return Err(error::Packet::NameLength {
                        actual: prefix + 1,
                        maximum: crate::host::Name::MAX_WIRE_LEN,
                    });
                }
                Ok(())
            })?;
        }
        self.position = position;
        Ok(Name {
            offset,
            packet: self.packet.proof,
        })
    }

    pub(in crate::wire) fn u16(&mut self) -> Result<u16, M::Error> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    pub(in crate::wire) fn u32(&mut self) -> Result<u32, M::Error> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], M::Error> {
        // SAFETY: unchecked cursors retain the exact immutable record traversal.
        let (bytes, end) = unsafe { M::array::<N>(self.packet.packet, self.position)? };
        self.position = end;
        Ok(bytes)
    }

    /// Normalizes ordinary TTLs while preserving OPT's extended response metadata.
    pub(in crate::wire) fn record(&mut self) -> Result<Record<'id>, M::Error> {
        let mut next = *self;
        let owner = next.name()?;
        let kind = records::Kind::from_wire(next.u16()?);
        let class = records::Class::from_wire(next.u16()?);
        let ttl = next.u32()?;
        let ttl = if kind != records::Kind::OPT && ttl & 0x8000_0000 != 0 {
            0
        } else {
            ttl
        };
        let length = next.u16()?;
        let start = next.position;
        next.bytes(length)?;
        let end = next.position;
        let record = Record {
            owner,
            kind,
            class,
            ttl,
            data: Span {
                start,
                end,
                packet: self.packet.proof,
            },
        };
        *self = next;
        Ok(record)
    }

    pub(in crate::wire) fn bytes(&mut self, length: u16) -> Result<&'packet [u8], M::Error> {
        // SAFETY: unchecked cursors retain the exact immutable record traversal.
        let read = unsafe { M::read(self.packet.packet, self.position, length)? };
        self.position = read.end;
        Ok(read.bytes)
    }
}

#[derive(Clone, Copy)]
pub(in crate::wire) struct Cursor<'packet, 'id, M = Checked> {
    pub(super) packet: Bound<'packet, 'id>,
    pub(super) position: Offset,
    pub(super) mode: marker::PhantomData<M>,
}

impl<'packet, 'id> Cursor<'packet, 'id> {
    pub(in crate::wire) fn records(
        &mut self,
        count: u16,
        mut read: impl FnMut(Record<'id>) -> Result<(), error::Packet>,
    ) -> Result<Records<'packet, 'id>, error::Packet> {
        let start = *self;
        for _ in 0..count {
            read(self.record()?)?;
        }
        Ok(Records {
            cursor: Cursor {
                packet: start.packet,
                position: start.position,
                mode: marker::PhantomData,
            },
            left: count,
        })
    }
}
