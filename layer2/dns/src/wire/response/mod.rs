mod aliases;

use std::mem;

use crate::{
    lookup::error,
    query::{answer::reply, family},
    wire::{Offset, cookies, packet, records, tx},
};

const HEADER_BYTES: Offset = Offset::new(12);
const BAD_COOKIE: u16 = 23;
const TRANSACTION_BYTES: usize = 2;

pub(crate) enum Datagram<'packet, const N: usize> {
    Authenticated {
        reply: reply::Reply<'packet, N>,
        server_cookie: cookies::Server,
    },
    BadCookie {
        server_cookie: cookies::Server,
    },
    StreamRequired,
}

struct Envelope<'packet, const N: usize> {
    reply: reply::Reply<'packet, N>,
    response_code: u16,
    server_cookie: Option<cookies::Server>,
}

pub(crate) struct Decoder<'packet> {
    packet: packet::Packet<'packet>,
    transaction: tx::Id,
}

impl<'packet> Decoder<'packet> {
    pub(crate) fn try_new(bytes: &'packet [u8]) -> Result<Self, error::Packet> {
        let packet = packet::Packet::try_new(bytes)?;
        let [a, b, ..] = bytes else {
            return Err(error::Packet::Truncated {
                offset: 0,
                actual: bytes.len(),
                required: TRANSACTION_BYTES,
            });
        };
        let id = tx::Id::from_wire(u16::from_be_bytes([*a, *b]));
        Ok(Self {
            packet,
            transaction: id,
        })
    }

    pub(crate) const fn transaction(&self) -> tx::Id {
        self.transaction
    }

    pub(crate) fn decode<const N: usize>(
        self,
        name: &crate::host::Name,
        family: family::Version,
    ) -> Result<reply::Reply<'packet, N>, error::Malformed> {
        self.body::<N>(name, family, None).map(|body| body.reply)
    }

    pub(crate) fn udp<const N: usize>(
        self,
        name: &crate::host::Name,
        family: family::Version,
        seed: cookies::Client,
    ) -> Result<Datagram<'packet, N>, error::Malformed> {
        let body = self.body::<N>(name, family, Some(seed))?;
        Ok(match body.server_cookie {
            Some(cookie) if body.response_code == BAD_COOKIE => Datagram::BadCookie {
                server_cookie: cookie,
            },
            Some(cookie) => Datagram::Authenticated {
                reply: body.reply,
                server_cookie: cookie,
            },
            None => Datagram::StreamRequired,
        })
    }

    fn body<const N: usize>(
        self,
        name: &crate::host::Name,
        family: family::Version,
        seed: Option<cookies::Client>,
    ) -> Result<Envelope<'packet, N>, error::Malformed> {
        self.packet
            .scope(|packet| Self::read(packet, name, family, seed))
    }

    fn read<const N: usize>(
        packet: packet::Bound<'packet, '_>,
        name: &crate::host::Name,
        family: family::Version,
        seed: Option<cookies::Client>,
    ) -> Result<Envelope<'packet, N>, error::Malformed> {
        let bytes = packet.bytes();
        let mut header = packet.cursor(Offset::new(2));
        let flags = header.u16().map_err(error::Malformed::Packet)?;
        if flags & 0x8000 == 0 || flags & 0x7800 != 0 {
            return Err(error::Malformed::Flags { actual: flags });
        }
        let query = header.u16().map_err(error::Malformed::Packet)?;
        let answer = header.u16().map_err(error::Malformed::Packet)?;
        let auth = header.u16().map_err(error::Malformed::Packet)?;
        let extra = header.u16().map_err(error::Malformed::Packet)?;
        if query != 1 {
            return Err(error::Malformed::QuestionCount {
                actual: query,
                expected: 1,
            });
        }

        let mut cursor = packet.cursor(HEADER_BYTES);
        let qname = cursor.name().map_err(error::Malformed::Packet)?;
        let qkind = records::Kind::from_wire(cursor.u16().map_err(error::Malformed::Packet)?);
        let class = records::Class::from_wire(cursor.u16().map_err(error::Malformed::Packet)?);
        if let Some(mismatch) = packet
            .compare_host(qname, name)
            .map_err(error::Malformed::Packet)?
        {
            return Err(error::Malformed::QuestionName(mismatch));
        }
        if qkind != family.kind() {
            return Err(error::Malformed::QuestionKind {
                actual: qkind.wire(),
                expected: family.kind().wire(),
            });
        }
        if class != records::Class::IN {
            return Err(error::Malformed::QuestionClass {
                actual: class.wire(),
                expected: records::Class::IN.wire(),
            });
        }

        let mut chain = aliases::Chain::new();
        let records = chain.read(&mut cursor, packet, answer)?;
        let mut ext = 0u8;
        let mut opt = false;
        let mut cookie = None;
        for (part, count) in [auth, extra].into_iter().enumerate() {
            let mut left = count;
            while left != 0 {
                left -= 1;
                let offset = cursor.position();
                let record = cursor.record().map_err(error::Malformed::Packet)?;
                if part == 1 && record.kind() == records::Kind::OPT {
                    if opt {
                        return Err(error::Malformed::OptDuplicate {
                            offset: offset.raw(),
                        });
                    }
                    if let Some(mismatch) = packet
                        .compare_root(record.owner())
                        .map_err(error::Malformed::Packet)?
                    {
                        return Err(error::Malformed::OptName(mismatch));
                    }
                    opt = true;
                    ext = (record.ttl() >> 24) as u8;
                    if let Some(seed) = seed {
                        cookie = cookies::Server::from_options(record.data.slice(packet), seed)
                            .map_err(error::Malformed::Cookie)?;
                    }
                }
            }
        }
        if cursor.position().get() != bytes.len() {
            return Err(error::Malformed::Trailing {
                offset: cursor.position().raw(),
                actual: bytes.len() - cursor.position().get(),
            });
        }

        let code = u16::from(ext) << 4 | (flags & 0x000f);
        let reply = if let Some(error) = chain.take() {
            reply::Reply::Malformed(error)
        } else if flags & 0x0200 != 0 {
            reply::Reply::Truncated
        } else {
            match code {
                0 => chain.resolve(packet, records, name, family)?,
                3 => reply::Reply::NameError,
                5 => reply::Reply::Refused,
                code => reply::Reply::Server { code },
            }
        };
        Ok(Envelope {
            reply,
            response_code: code,
            server_cookie: cookie,
        })
    }
}

const _: () = assert!(!mem::needs_drop::<Decoder<'static>>());
