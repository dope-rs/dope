use std::{convert::Infallible, marker, mem};

use o3::base::cell::region;

use crate::{
    host,
    lookup::error,
    wire::{Offset, records},
};

const MAX_POINTER_HOPS: usize = 32;

mod expanded;
pub(in crate::wire) mod raw;

pub(crate) use expanded::Expanded;

fn validated<T>(result: Result<T, Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

#[derive(Clone, Copy)]
pub(in crate::wire) struct Packet<'packet> {
    bytes: &'packet [u8],
}

#[derive(Clone, Copy)]
pub(in crate::wire) struct Bound<'packet, 'id> {
    packet: Packet<'packet>,
    proof: region::Proof<region::Identity<'id>>,
}

#[derive(Clone, Copy)]
pub(in crate::wire) struct Name<'id> {
    offset: Offset,
    packet: region::Proof<region::Identity<'id>>,
}

#[derive(Clone, Copy)]
pub(in crate::wire) struct Record<'id> {
    owner: Name<'id>,
    kind: records::Kind,
    class: records::Class,
    ttl: u32,
    pub(in crate::wire) data: raw::Span<'id>,
}

/// The exact immutable record sequence already traversed by a checked cursor.
pub(in crate::wire) struct Records<'packet, 'id> {
    cursor: raw::Cursor<'packet, 'id, raw::Validated>,
    left: u16,
}

impl<'id> Iterator for Records<'_, 'id> {
    type Item = Record<'id>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(validated(self.cursor.record()))
    }
}

#[derive(Clone)]
struct Labels<'packet> {
    packet: Packet<'packet>,
    position: Offset,
    pointer_hops: usize,
    expanded_len: usize,
}

pub(in crate::wire) struct Read<'packet> {
    bytes: &'packet [u8],
    end: Offset,
}

enum Part<'packet> {
    Label(Read<'packet>),
    Pointer { target: Offset, end: Offset },
}

impl Part<'_> {
    const fn end(&self) -> Offset {
        match self {
            Self::Label(read) => read.end,
            Self::Pointer { end, .. } => *end,
        }
    }
}

impl<'packet> Packet<'packet> {
    pub(in crate::wire) fn try_new(bytes: &'packet [u8]) -> Result<Self, error::Packet> {
        if bytes.len() > usize::from(u16::MAX) {
            return Err(error::Packet::Length {
                actual: bytes.len(),
                maximum: usize::from(u16::MAX),
            });
        }
        Ok(Self { bytes })
    }

    pub(in crate::wire) fn scope<R>(self, f: impl for<'id> FnOnce(Bound<'packet, 'id>) -> R) -> R {
        region::Token::scope(|token| {
            f(Bound {
                packet: self,
                proof: token.proof(),
            })
        })
    }
}

impl<'packet, 'id> Bound<'packet, 'id> {
    pub(in crate::wire) const fn bytes(self) -> &'packet [u8] {
        self.packet.bytes
    }

    pub(in crate::wire) const fn cursor(self, position: Offset) -> raw::Cursor<'packet, 'id> {
        raw::Cursor {
            packet: self,
            position,
            mode: marker::PhantomData,
        }
    }

    pub(in crate::wire) fn eq_host(
        self,
        name: Name<'id>,
        expected: &crate::host::Name,
    ) -> Result<bool, error::Packet> {
        let mut actual = self.labels(name);
        let mut expected = expected.labels();
        loop {
            match (actual.next().transpose()?, expected.next()) {
                (Some(actual), Some(expected)) => {
                    if !actual.eq_ignore_ascii_case(expected) {
                        return Ok(false);
                    }
                }
                (None, None) => return Ok(true),
                _ => return Ok(false),
            }
        }
    }

    pub(in crate::wire) fn compare_host(
        self,
        name: Name<'id>,
        expected: &crate::host::Name,
    ) -> Result<Option<host::Mismatch>, error::Packet> {
        let mut actual = self.labels(name);
        let mut expected = expected.labels();
        let mut index = 0;
        loop {
            match (actual.next().transpose()?, expected.next()) {
                (Some(actual), Some(expected)) => {
                    if actual.len() != expected.len() {
                        return Ok(Some(host::Mismatch::LabelLength {
                            index,
                            actual: actual.len(),
                            expected: expected.len(),
                        }));
                    }
                    for (offset, (actual, expected)) in actual.iter().zip(expected).enumerate() {
                        if !actual.eq_ignore_ascii_case(expected) {
                            return Ok(Some(host::Mismatch::LabelByte {
                                index,
                                offset,
                                actual: *actual,
                                expected: *expected,
                            }));
                        }
                    }
                }
                (None, None) => return Ok(None),
                (Some(actual), None) => {
                    return Ok(Some(host::Mismatch::ExtraLabel {
                        index,
                        actual: actual.len(),
                    }));
                }
                (None, Some(expected)) => {
                    return Ok(Some(host::Mismatch::MissingLabel {
                        index,
                        expected: expected.len(),
                    }));
                }
            }
            index += 1;
        }
    }

    pub(in crate::wire) fn eq_name(
        self,
        actual: Name<'id>,
        expected: Name<'id>,
    ) -> Result<bool, error::Packet> {
        let mut actual = self.labels(actual);
        let mut expected = self.labels(expected);
        loop {
            match (actual.next().transpose()?, expected.next().transpose()?) {
                (Some(actual), Some(expected)) => {
                    if !actual.eq_ignore_ascii_case(expected) {
                        return Ok(false);
                    }
                }
                (None, None) => return Ok(true),
                _ => return Ok(false),
            }
        }
    }

    pub(in crate::wire) fn compare_root(
        self,
        name: Name<'id>,
    ) -> Result<Option<host::Mismatch>, error::Packet> {
        Ok(self
            .labels(name)
            .next()
            .transpose()?
            .map(|actual| host::Mismatch::ExtraLabel {
                index: 0,
                actual: actual.len(),
            }))
    }

    pub(in crate::wire) fn expand(
        self,
        name: Name<'id>,
    ) -> Result<Expanded<'packet>, error::Packet> {
        Expanded::try_new(self.labels(name))
    }

    fn labels(self, name: Name<'id>) -> Labels<'packet> {
        let Name {
            offset,
            packet: _proof,
        } = name;
        Labels {
            packet: self.packet,
            position: offset,
            pointer_hops: 0,
            expanded_len: 0,
        }
    }
}

impl<'id> Record<'id> {
    pub(in crate::wire) const fn owner(self) -> Name<'id> {
        self.owner
    }

    pub(in crate::wire) const fn kind(self) -> records::Kind {
        self.kind
    }

    pub(in crate::wire) const fn class(self) -> records::Class {
        self.class
    }

    pub(in crate::wire) const fn ttl(self) -> u32 {
        self.ttl
    }

    pub(in crate::wire) fn name<'packet>(
        self,
        packet: Bound<'packet, 'id>,
    ) -> Result<Name<'id>, error::Packet> {
        let mut cursor = packet.cursor(self.data.start);
        let name = cursor.name()?;
        if cursor.position() != self.data.end {
            return Err(error::Packet::RecordName {
                actual: cursor.position().raw(),
                expected: self.data.end.raw(),
            });
        }
        Ok(name)
    }
}

impl<'packet> Labels<'packet> {
    fn fail(error: error::Packet) -> Option<Result<&'packet [u8], error::Packet>> {
        Some(Err(error))
    }
}

impl<'packet> Iterator for Labels<'packet> {
    type Item = Result<&'packet [u8], error::Packet>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.clone();
        loop {
            let part = match (raw::Label {
                packet: next.packet,
                position: next.position,
            })
            .try_read()
            {
                Ok(part) => part,
                Err(error) => return Self::fail(error),
            };
            let label = match part {
                Part::Pointer { target, .. } => {
                    next.pointer_hops += 1;
                    if next.pointer_hops > MAX_POINTER_HOPS {
                        return Self::fail(error::Packet::PointerHops {
                            actual: next.pointer_hops,
                            maximum: MAX_POINTER_HOPS,
                        });
                    }
                    next.position = target;
                    continue;
                }
                Part::Label(label) => label,
            };
            if label.bytes.is_empty() {
                return None;
            }
            next.position = label.end;
            let total = next.expanded_len + label.bytes.len() + 1;
            if total + 1 > crate::host::Name::MAX_WIRE_LEN {
                return Self::fail(error::Packet::NameLength {
                    actual: total + 1,
                    maximum: crate::host::Name::MAX_WIRE_LEN,
                });
            }
            next.expanded_len = total;
            *self = next;
            return Some(Ok(label.bytes));
        }
    }
}

fn bytes<'packet>(
    packet: Packet<'packet>,
    offset: Offset,
    required: u16,
) -> Result<Read<'packet>, error::Packet> {
    let input = packet.bytes;
    let offset = offset.get();
    let required = usize::from(required);
    let actual = input.len().saturating_sub(offset);
    let end = offset + required;
    let value = input.get(offset..end).ok_or(error::Packet::Truncated {
        offset,
        required,
        actual,
    })?;
    Ok(Read {
        bytes: value,
        end: Offset::new(end as u16),
    })
}

const _: () = {
    assert!(mem::size_of::<Name<'static>>() == mem::size_of::<u16>());
    assert!(mem::size_of::<Record<'static>>() == 16);
    assert!(mem::size_of::<raw::Cursor<'static, 'static>>() == 3 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<raw::Cursor<'static, 'static, raw::Validated>>()
            == mem::size_of::<raw::Cursor<'static, 'static>>()
    );
    assert!(!mem::needs_drop::<Packet<'static>>());
    assert!(!mem::needs_drop::<raw::Cursor<'static, 'static>>());
};
