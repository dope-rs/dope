use std::{marker, net};

use o3::collections::fixed::array;

use crate::wire;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum DecodeError {
    Header,
    Question,
    Name,
    Record,
    Integer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Rejection {
    Capacity { actual: u8 },
    CnameRecords,
    CnameDepth,
    InvalidAlias,
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "a transient bounded decode result moves inline into fixed query state without allocation"
)]
pub(crate) enum Decoded<const N: usize> {
    Outcome(wire::Outcome<N>),
    Rejected(Rejection),
}

struct Decoder<'packet> {
    packet: &'packet [u8],
}

/// Packet-bounded DNS name identity. Only the offset is retained; labels borrow
/// the packet transiently while they are validated, compared, or materialized.
#[derive(Clone, Copy)]
struct Name<'packet> {
    offset: u16,
    packet: marker::PhantomData<&'packet [u8]>,
}

struct Labels<'packet> {
    packet: &'packet [u8],
    position: usize,
    continuation: Option<usize>,
    end: Option<usize>,
    pointer_hops: usize,
    expanded_len: usize,
    done: bool,
}

/// Fixed-size CNAME graph edge retained only for one response decode.
#[derive(Clone, Copy)]
struct Alias<'packet> {
    owner: Name<'packet>,
    target: Name<'packet>,
    ttl: u32,
}

#[derive(Clone, Copy)]
enum CurrentName<'packet, 'expected> {
    Expected(&'expected crate::Name),
    Wire(Name<'packet>),
}

const _: () = assert!(std::mem::size_of::<Name<'static>>() == std::mem::size_of::<u16>());
const _: () = assert!(std::mem::size_of::<Alias<'static>>() == 8);
const _: () = assert!(std::mem::size_of::<DecodeError>() == 1);
const _: () = assert!(std::mem::size_of::<Rejection>() == 2);

impl<'packet> Decoder<'packet> {
    const fn new(packet: &'packet [u8]) -> Self {
        Self { packet }
    }

    /// Advances over the encoded name without expanding compression targets.
    /// Bounds and the backward-pointer invariant are checked immediately;
    /// expanded-name validation is paid only when a name affects the query.
    fn scan_name(&self, cursor: &mut usize) -> Result<Name<'packet>, DecodeError> {
        let offset = u16::try_from(*cursor).map_err(|_| DecodeError::Name)?;
        let mut position = *cursor;
        let mut expanded_prefix = 0usize;
        loop {
            let &length = self.packet.get(position).ok_or(DecodeError::Name)?;
            if length & 0xc0 == 0xc0 {
                let &low = self.packet.get(position + 1).ok_or(DecodeError::Name)?;
                let target = usize::from(u16::from_be_bytes([length & 0x3f, low]));
                if target >= self.packet.len() || target >= position {
                    return Err(DecodeError::Name);
                }
                position += 2;
                break;
            }
            if length & 0xc0 != 0 {
                return Err(DecodeError::Name);
            }
            position += 1;
            if length == 0 {
                break;
            }
            position = position
                .checked_add(usize::from(length))
                .filter(|end| *end <= self.packet.len())
                .ok_or(DecodeError::Name)?;
            expanded_prefix = expanded_prefix
                .checked_add(usize::from(length) + 1)
                .filter(|expanded| *expanded < crate::Name::MAX_WIRE_LEN)
                .ok_or(DecodeError::Name)?;
        }
        *cursor = position;
        Ok(Name {
            offset,
            packet: marker::PhantomData,
        })
    }

    fn labels(&self, name: Name<'packet>) -> Labels<'packet> {
        Labels {
            packet: self.packet,
            position: usize::from(name.offset),
            continuation: None,
            end: None,
            pointer_hops: 0,
            expanded_len: 0,
            done: false,
        }
    }

    fn eq_host(&self, name: Name<'packet>, expected: &crate::Name) -> Result<bool, DecodeError> {
        let mut actual = self.labels(name);
        let mut expected = expected.labels();
        loop {
            match (actual.next().transpose()?, expected.next()) {
                (Some(actual), Some(expected)) if actual.eq_ignore_ascii_case(expected) => {}
                (None, None) => return Ok(true),
                (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => return Ok(false),
            }
        }
    }

    fn eq(
        &self,
        name: Name<'packet>,
        expected: CurrentName<'packet, '_>,
    ) -> Result<bool, DecodeError> {
        let expected = match expected {
            CurrentName::Expected(expected) => return self.eq_host(name, expected),
            CurrentName::Wire(expected) => expected,
        };
        let mut actual = self.labels(name);
        let mut expected = self.labels(expected);
        loop {
            match (actual.next().transpose()?, expected.next().transpose()?) {
                (Some(actual), Some(expected)) if actual.eq_ignore_ascii_case(expected) => {}
                (None, None) => return Ok(true),
                (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => return Ok(false),
            }
        }
    }

    fn to_host(&self, name: Name<'packet>) -> Result<Option<crate::Name>, DecodeError> {
        let mut values: [&[u8]; 128] = [&[]; 128];
        let mut len = 0;
        for label in self.labels(name) {
            if len == values.len() {
                return Ok(None);
            }
            values[len] = label?;
            len += 1;
        }
        Ok(crate::Name::from_wire_labels(&values[..len]).ok())
    }
}

impl<'packet> Labels<'packet> {
    fn fail(&mut self) -> Option<Result<&'packet [u8], DecodeError>> {
        self.done = true;
        Some(Err(DecodeError::Name))
    }
}

impl<'packet> Iterator for Labels<'packet> {
    type Item = Result<&'packet [u8], DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            let Some(&length) = self.packet.get(self.position) else {
                return self.fail();
            };
            if length & 0xc0 == 0xc0 {
                let Some(&low) = self.packet.get(self.position + 1) else {
                    return self.fail();
                };
                let target = usize::from(u16::from_be_bytes([length & 0x3f, low]));
                if target >= self.packet.len() || target >= self.position {
                    return self.fail();
                }
                if self.continuation.is_none() {
                    self.continuation = Some(self.position + 2);
                }
                self.position = target;
                self.pointer_hops += 1;
                if self.pointer_hops > wire::MAX_POINTER_HOPS {
                    return self.fail();
                }
                continue;
            }
            if length & 0xc0 != 0 {
                return self.fail();
            }
            self.position += 1;
            if length == 0 {
                if self.expanded_len + 1 > crate::Name::MAX_WIRE_LEN {
                    return self.fail();
                }
                self.end = Some(self.continuation.unwrap_or(self.position));
                self.done = true;
                return None;
            }
            let Some(end) = self
                .position
                .checked_add(usize::from(length))
                .filter(|end| *end <= self.packet.len())
            else {
                return self.fail();
            };
            let Some(expanded_len) = self
                .expanded_len
                .checked_add(usize::from(length) + 1)
                .filter(|expanded| *expanded < crate::Name::MAX_WIRE_LEN)
            else {
                return self.fail();
            };
            self.expanded_len = expanded_len;
            let label = &self.packet[self.position..end];
            self.position = end;
            return Some(Ok(label));
        }
    }
}

pub(crate) struct Response<'a> {
    packet: &'a [u8],
    expected_id: wire::TransactionId,
    expected_name: &'a crate::Name,
    expected_type: u16,
}

impl<'a> Response<'a> {
    pub(crate) const fn new(
        packet: &'a [u8],
        expected_id: wire::TransactionId,
        expected_name: &'a crate::Name,
        expected_type: u16,
    ) -> Self {
        Self {
            packet,
            expected_id,
            expected_name,
            expected_type,
        }
    }

    pub(crate) fn id(packet: &[u8]) -> Option<wire::TransactionId> {
        packet
            .get(..2)
            .map(|bytes| wire::TransactionId::from_wire(u16::from_be_bytes([bytes[0], bytes[1]])))
    }

    pub(crate) fn decode<const N: usize>(self) -> Result<Decoded<N>, DecodeError> {
        let packet = self.packet;
        let decoder = Decoder::new(packet);
        if packet.len() < wire::HEADER_LEN || Self::id(packet) != Some(self.expected_id) {
            return Err(DecodeError::Header);
        }
        let flags = Self::read_u16(packet, 2)?;
        if flags & 0x8000 == 0 || flags & 0x7800 != 0 {
            return Err(DecodeError::Header);
        }
        if flags & 0x0200 != 0 {
            return Ok(Decoded::Outcome(wire::Outcome::Truncated));
        }
        let rcode = (flags & 0x000f) as u8;
        if rcode != 0 {
            return Ok(Decoded::Outcome(wire::Outcome::Failed(match rcode {
                1 => wire::Rcode::Format,
                2 => wire::Rcode::Server,
                3 => wire::Rcode::Name,
                4 => wire::Rcode::NotImplemented,
                5 => wire::Rcode::Refused,
                value => wire::Rcode::Other(value),
            })));
        }
        let questions = usize::from(Self::read_u16(packet, 4)?);
        let answers = usize::from(Self::read_u16(packet, 6)?);
        let authorities = usize::from(Self::read_u16(packet, 8)?);
        let additional = usize::from(Self::read_u16(packet, 10)?);
        if questions != 1 {
            return Err(DecodeError::Question);
        }
        let mut cursor = wire::HEADER_LEN;
        let question = decoder.scan_name(&mut cursor)?;
        let question_type = Self::take_u16(packet, &mut cursor)?;
        let question_class = Self::take_u16(packet, &mut cursor)?;
        if !decoder.eq_host(question, self.expected_name)?
            || question_type != self.expected_type
            || question_class != wire::IN
        {
            return Err(DecodeError::Question);
        }

        let answers_offset = cursor;
        let mut aliases = array::Inline::<Alias<'a>, { wire::MAX_CNAME_RECORDS }>::new();
        for section in 0..3 {
            let count = match section {
                0 => answers,
                1 => authorities,
                _ => additional,
            };
            for _ in 0..count {
                let owner = decoder.scan_name(&mut cursor)?;
                let record_type = Self::take_u16(packet, &mut cursor)?;
                let class = Self::take_u16(packet, &mut cursor)?;
                let ttl = Self::take_u32(packet, &mut cursor)?;
                let len = usize::from(Self::take_u16(packet, &mut cursor)?);
                let end = cursor
                    .checked_add(len)
                    .filter(|end| *end <= packet.len())
                    .ok_or(DecodeError::Record)?;
                if section == 0 && class == wire::IN && record_type == wire::CNAME {
                    let mut name_cursor = cursor;
                    let target = decoder.scan_name(&mut name_cursor)?;
                    if name_cursor != end {
                        return Err(DecodeError::Record);
                    }
                    if aliases.push(Alias { owner, target, ttl }).is_err() {
                        return Ok(Decoded::Rejected(Rejection::CnameRecords));
                    }
                }
                cursor = end;
            }
        }
        Self::resolve(
            &decoder,
            answers_offset,
            answers,
            self.expected_name,
            self.expected_type,
            aliases,
        )
    }

    fn resolve<const N: usize>(
        decoder: &Decoder<'_>,
        answers_offset: usize,
        answer_count: usize,
        expected: &crate::Name,
        expected_type: u16,
        aliases: array::Inline<Alias<'_>, { wire::MAX_CNAME_RECORDS }>,
    ) -> Result<Decoded<N>, DecodeError> {
        let packet = decoder.packet;
        let mut current = CurrentName::Expected(expected);
        let mut chain_ttl = u32::MAX;
        let mut alias_hops = 0;
        loop {
            let mut next = None;
            for alias in &aliases {
                if decoder.eq(alias.owner, current)? {
                    next = Some(*alias);
                    break;
                }
            }
            let Some(alias) = next else {
                break;
            };
            if alias_hops == wire::MAX_CNAME_DEPTH {
                return Ok(Decoded::Rejected(Rejection::CnameDepth));
            }
            chain_ttl = chain_ttl.min(alias.ttl);
            current = CurrentName::Wire(alias.target);
            alias_hops += 1;
        }

        let mut cursor = answers_offset;
        let mut values = crate::Addresses::new();
        let mut ttl = chain_ttl;
        for _ in 0..answer_count {
            let owner = decoder.scan_name(&mut cursor)?;
            let record_type = Self::take_u16(packet, &mut cursor)?;
            let class = Self::take_u16(packet, &mut cursor)?;
            let record_ttl = Self::take_u32(packet, &mut cursor)?;
            let len = usize::from(Self::take_u16(packet, &mut cursor)?);
            let end = cursor
                .checked_add(len)
                .filter(|end| *end <= packet.len())
                .ok_or(DecodeError::Record)?;
            if class == wire::IN
                && record_type == expected_type
                && decoder.eq(owner, current)?
                && let Some(value) = Self::address(packet, cursor, end, record_type)
            {
                match values.try_insert_unique(value) {
                    Ok(true) => ttl = ttl.min(record_ttl),
                    Ok(false) => {}
                    Err(_) => {
                        let actual =
                            u8::try_from(values.len() + 1).map_err(|_| DecodeError::Record)?;
                        return Ok(Decoded::Rejected(Rejection::Capacity { actual }));
                    }
                }
            }
            cursor = end;
        }
        if !values.is_empty() {
            return Ok(Decoded::Outcome(wire::Outcome::Addresses {
                values,
                ttl: if ttl == u32::MAX { 0 } else { ttl },
                alias_hops: alias_hops as u8,
            }));
        }
        match current {
            CurrentName::Expected(_) => Ok(Decoded::Outcome(wire::Outcome::NoData)),
            CurrentName::Wire(name) => match decoder.to_host(name)? {
                Some(name) => Ok(Decoded::Outcome(wire::Outcome::Alias {
                    name,
                    ttl: if chain_ttl == u32::MAX { 0 } else { chain_ttl },
                    hops: alias_hops as u8,
                })),
                None => Ok(Decoded::Rejected(Rejection::InvalidAlias)),
            },
        }
    }

    fn address(packet: &[u8], start: usize, end: usize, record_type: u16) -> Option<net::IpAddr> {
        match (record_type, end.checked_sub(start)?) {
            (wire::A, 4) => Some(net::IpAddr::from([
                packet[start],
                packet[start + 1],
                packet[start + 2],
                packet[start + 3],
            ])),
            (wire::AAAA, 16) => {
                let mut octets = [0; 16];
                octets.copy_from_slice(&packet[start..end]);
                Some(net::IpAddr::from(octets))
            }
            _ => None,
        }
    }

    fn read_u16(packet: &[u8], offset: usize) -> Result<u16, DecodeError> {
        let bytes = packet.get(offset..offset + 2).ok_or(DecodeError::Integer)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn take_u16(packet: &[u8], cursor: &mut usize) -> Result<u16, DecodeError> {
        let value = Self::read_u16(packet, *cursor)?;
        *cursor += 2;
        Ok(value)
    }

    fn take_u32(packet: &[u8], cursor: &mut usize) -> Result<u32, DecodeError> {
        let bytes = packet
            .get(*cursor..*cursor + 4)
            .ok_or(DecodeError::Integer)?;
        *cursor += 4;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}
