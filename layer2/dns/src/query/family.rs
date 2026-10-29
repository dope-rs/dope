use std::net;

use crate::wire::records;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Version {
    Four = 0,
    Six = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    pub version: Version,
    pub actual: usize,
    pub expected: usize,
}

impl Version {
    pub(crate) const ALL: [Self; 2] = [Self::Four, Self::Six];
    pub(crate) const COUNT: usize = Self::ALL.len();

    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    pub(crate) const fn kind(self) -> records::Kind {
        match self {
            Self::Four => records::Kind::A,
            Self::Six => records::Kind::AAAA,
        }
    }

    pub(crate) fn decode(self, data: &[u8]) -> Result<net::IpAddr, Error> {
        match self {
            Self::Four => {
                let [a, b, c, d] = data else {
                    return Err(Error {
                        version: self,
                        actual: data.len(),
                        expected: 4,
                    });
                };
                Ok(net::IpAddr::from([*a, *b, *c, *d]))
            }
            Self::Six => {
                let Ok(bytes) = <[u8; 16]>::try_from(data) else {
                    return Err(Error {
                        version: self,
                        actual: data.len(),
                        expected: 16,
                    });
                };
                Ok(net::IpAddr::from(bytes))
            }
        }
    }

    pub(crate) const fn from_bit(bit: u16) -> Self {
        if bit == 0 { Self::Four } else { Self::Six }
    }
}
