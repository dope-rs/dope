pub mod app;
pub mod attempt;
pub mod auxiliary;
pub mod session;

pub mod codec;
pub mod connection;
pub mod lifecycle;
pub mod port;

pub use connection::Engine;

use crate::receive::ingress;

pub const IOV_CAP: usize = ingress::IOV_CAP;
