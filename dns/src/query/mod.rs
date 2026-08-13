pub mod family;
pub mod server;

use crate::transport;

/// Complete DNS behavior locked to the zero-sized combination accepted by [`Self::new`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    transport: transport::DatagramThenStream,
    server: server::Cycle,
    family: family::RequireAll,
}

impl Policy {
    pub const fn new(
        transport: transport::DatagramThenStream,
        server: server::Cycle,
        family: family::RequireAll,
    ) -> Self {
        Self {
            transport,
            server,
            family,
        }
    }
}

const _: () = assert!(std::mem::size_of::<Policy>() == 0);
