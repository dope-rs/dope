//! Authenticated return tickets for an exclusively owned relay worker.

use std::{mem::MaybeUninit, net};

use dope::core::{Backend, Errno};
use dope_sys::thread::Thread as _;
use o3::base::marker::ThreadBound;
use subtle::ConstantTimeEq as _;
use zeroize::{Zeroize as _, Zeroizing};

use super::secrets::{self, Context};

const BODY_LEN: usize = 15;
const WIRE_LEN: usize = BODY_LEN + secrets::TOKEN_TAG_LEN;
const IPV4: u8 = 0x14;
const IPV6: u8 = 0x16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    Length { actual: usize },
    Marker(u8),
    Ipv4 { flowinfo: u32, scope_id: u32 },
    Source { marker: u8, address: net::IpAddr },
    Authentication,
}

/// An admitted wire format, not proof that a relay authenticated its return path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Ticket([u8; WIRE_LEN]);

impl Ticket {
    pub const LEN: usize = WIRE_LEN;
    pub(crate) const MARKERS: [u8; 2] = [IPV4, IPV6];

    pub const fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }

    /// Selects the original worker's immutable canonical source allowlist.
    pub const fn source_index(&self) -> u32 {
        u32::from_be_bytes([self.0[1], self.0[2], self.0[3], self.0[4]])
    }

    const fn port(&self) -> u16 {
        u16::from_be_bytes([self.0[5], self.0[6]])
    }

    const fn flowinfo(&self) -> u32 {
        u32::from_be_bytes([self.0[7], self.0[8], self.0[9], self.0[10]])
    }

    const fn scope_id(&self) -> u32 {
        u32::from_be_bytes([self.0[11], self.0[12], self.0[13], self.0[14]])
    }

    fn source(&self, canonical: net::IpAddr) -> Result<net::SocketAddr, Reject> {
        let address = canonical.to_canonical();
        let addr = match (self.0[0], address) {
            (IPV4, net::IpAddr::V4(ip)) => {
                net::SocketAddr::V4(net::SocketAddrV4::new(ip, self.port()))
            }
            (IPV4, address) => {
                return Err(Reject::Source {
                    marker: IPV4,
                    address,
                });
            }
            (IPV6, address) => {
                let ip = match address {
                    net::IpAddr::V4(ip) => ip.to_ipv6_mapped(),
                    net::IpAddr::V6(ip) => ip,
                };
                net::SocketAddr::V6(net::SocketAddrV6::new(
                    ip,
                    self.port(),
                    self.flowinfo(),
                    self.scope_id(),
                ))
            }
            _ => unreachable!("ticket"),
        };
        Ok(addr)
    }
}

impl TryFrom<&[u8; Ticket::LEN]> for Ticket {
    type Error = Reject;

    fn try_from(bytes: &[u8; Self::LEN]) -> Result<Self, Self::Error> {
        let ticket = Self(*bytes);
        match bytes[0] {
            IPV4 => {
                let flowinfo = ticket.flowinfo();
                let scope_id = ticket.scope_id();
                if flowinfo != 0 || scope_id != 0 {
                    return Err(Reject::Ipv4 { flowinfo, scope_id });
                }
            }
            IPV6 => {}
            marker => return Err(Reject::Marker(marker)),
        }
        Ok(ticket)
    }
}

impl TryFrom<&[u8]> for Ticket {
    type Error = Reject;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let fixed: &[u8; Self::LEN] = bytes.try_into().map_err(|_| Reject::Length {
            actual: bytes.len(),
        })?;
        Self::try_from(fixed)
    }
}

/// Fresh boot secret held only by the relay worker that owns both sockets and
/// its immutable allowlist. No key material is exported or cloned.
pub struct Key {
    secret: [u8; 32],
    _bound: ThreadBound,
}

impl Key {
    pub fn try_new() -> Result<Self, Errno> {
        let mut material = Zeroizing::new(MaybeUninit::uninit());
        let secret = *Backend::entropy::<32>(&mut material)?;
        Ok(Self {
            secret,
            _bound: ThreadBound::NEW,
        })
    }

    pub fn issue(&self, index: u32, original: net::SocketAddr) -> Ticket {
        let mut ticket = Ticket([0; Ticket::LEN]);
        ticket.0[1..5].copy_from_slice(&index.to_be_bytes());
        ticket.0[5..7].copy_from_slice(&original.port().to_be_bytes());
        match original {
            net::SocketAddr::V4(_) => ticket.0[0] = IPV4,
            net::SocketAddr::V6(addr) => {
                ticket.0[0] = IPV6;
                ticket.0[7..11].copy_from_slice(&addr.flowinfo().to_be_bytes());
                ticket.0[11..15].copy_from_slice(&addr.scope_id().to_be_bytes());
            }
        }
        let tag = self.tag(&ticket, original);
        ticket.0[BODY_LEN..].copy_from_slice(&tag);
        ticket
    }

    pub fn verify(
        &self,
        ticket: &Ticket,
        canonical: net::IpAddr,
    ) -> Result<net::SocketAddr, Reject> {
        let original = ticket.source(canonical)?;
        let expected = self.tag(ticket, original);
        if !bool::from(ticket.0[BODY_LEN..].ct_eq(&expected)) {
            return Err(Reject::Authentication);
        }
        Ok(original)
    }

    fn tag(&self, ticket: &Ticket, original: net::SocketAddr) -> [u8; secrets::TOKEN_TAG_LEN] {
        let mut ctx = Context::new(&self.secret);
        ctx.update(b"qrelayreturn-v1");
        ctx.update(&ticket.0[..BODY_LEN]);
        ctx.update(&secrets::encode_socket_addr(original));
        ctx.finish()
    }
}

impl Drop for Key {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}
