use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use dope_core::io::socket;

pub mod driver;
pub mod file;
pub mod wake;

pub mod contract;
pub mod rt;
pub mod timer;

pub const DATAGRAM: socket::Config = socket::Config::for_datagram(None);
pub const LOCALHOST: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
