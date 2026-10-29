use std::mem;

use o3::depot::array::copy;

use crate::{
    lookup::error,
    query::{
        answer::{addr, reply},
        family,
    },
    wire::{packet, records},
};

const MAX_RECORDS: usize = 32;

#[derive(Clone, Copy)]
struct Edge<'id> {
    owner: packet::Name<'id>,
    target: packet::Name<'id>,
    ttl: u32,
}

#[derive(Clone, Copy)]
enum Current<'id> {
    Expected,
    Wire(packet::Name<'id>),
}

impl<'id> Current<'id> {
    fn matches(
        self,
        packet: packet::Bound<'_, 'id>,
        actual: packet::Name<'id>,
        expected: &crate::host::Name,
    ) -> Result<bool, error::Malformed> {
        match self {
            Self::Expected => packet.eq_host(actual, expected),
            Self::Wire(name) => packet.eq_name(actual, name),
        }
        .map_err(error::Malformed::Packet)
    }
}

pub(super) struct Chain<'id> {
    edges: copy::Vec<Edge<'id>, MAX_RECORDS>,
    aliases: u16,
}

impl<'id> Chain<'id> {
    pub(super) fn new() -> Self {
        Self {
            edges: copy::Vec::new(),
            aliases: 0,
        }
    }

    pub(super) fn read<'packet>(
        &mut self,
        cursor: &mut packet::raw::Cursor<'packet, 'id>,
        packet: packet::Bound<'packet, 'id>,
        count: u16,
    ) -> Result<packet::Records<'packet, 'id>, error::Malformed> {
        cursor
            .records(count, |record| {
                if record.class() != records::Class::IN || record.kind() != records::Kind::CNAME {
                    return Ok(());
                }
                let target = record.name(packet)?;
                self.aliases += 1;
                if let Some(vacant) = self.edges.vacancy() {
                    vacant.insert(Edge {
                        owner: record.owner(),
                        target,
                        ttl: record.ttl(),
                    });
                }
                Ok(())
            })
            .map_err(error::Malformed::Packet)
    }

    pub(super) fn take(&mut self) -> Option<error::Malformed> {
        (usize::from(self.aliases) > MAX_RECORDS).then_some(error::Malformed::AliasRecords {
            actual: usize::from(self.aliases),
            maximum: MAX_RECORDS,
        })
    }

    pub(super) fn resolve<'packet, const N: usize>(
        self,
        packet: packet::Bound<'packet, 'id>,
        records: packet::Records<'packet, 'id>,
        name: &crate::host::Name,
        family: family::Version,
    ) -> Result<reply::Reply<'packet, N>, error::Malformed> {
        let mut owner = Current::Expected;
        let mut ttl = u32::MAX;
        let mut hops = reply::Hops::ZERO;
        loop {
            let mut next: Option<Edge<'id>> = None;
            for edge in &self.edges {
                if owner.matches(packet, edge.owner, name)? {
                    match &mut next {
                        Some(first) => {
                            if !packet
                                .eq_name(first.target, edge.target)
                                .map_err(error::Malformed::Packet)?
                            {
                                return Err(error::Malformed::AliasConflict);
                            }
                            first.ttl = first.ttl.min(edge.ttl);
                        }
                        None => next = Some(*edge),
                    }
                }
            }
            let Some(edge) = next else {
                break;
            };
            hops = match hops.advance() {
                Ok(hops) => hops,
                Err(actual) => {
                    return Ok(reply::Reply::Malformed(error::Malformed::AliasHops {
                        actual,
                        maximum: reply::MAX_ALIAS_HOPS,
                    }));
                }
            };
            ttl = ttl.min(edge.ttl);
            owner = Current::Wire(edge.target);
        }

        let mut values = addr::Addresses::new();
        let mut min = ttl;
        for record in records {
            let same = owner.matches(packet, record.owner(), name)?;
            if record.class() == records::Class::IN && record.kind() == family.kind() && same {
                let value = family
                    .decode(record.data.slice(packet))
                    .map_err(error::Malformed::Address)?;
                match values.insert(value) {
                    addr::Insert::Added | addr::Insert::Present => {
                        min = min.min(record.ttl());
                    }
                    addr::Insert::Full => {
                        return Ok(reply::Reply::Capacity {
                            actual: values.len() + 1,
                        });
                    }
                }
            }
        }
        if !values.is_empty() {
            return Ok(reply::Reply::Addresses {
                values,
                ttl: min,
                alias_hops: hops,
            });
        }
        match owner {
            Current::Expected => Ok(reply::Reply::NoData),
            Current::Wire(name) => Ok(reply::Reply::Alias {
                name: packet.expand(name).map_err(error::Malformed::Packet)?,
                ttl,
                hops,
            }),
        }
    }
}

const _: () = {
    assert!(MAX_RECORDS < u16::MAX as usize);
    assert!(mem::size_of::<Edge<'static>>() == 8);
    assert!(!mem::needs_drop::<Chain<'static>>());
};
