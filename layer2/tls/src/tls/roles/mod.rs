use std::sync::Arc;

use o3::alloc::{BoxSliceExt as _, array::Block};

use crate::tls::client;
pub(super) use contract::Contract;

mod contract;

pub struct Server;

pub struct Client;

/// Closed endpoint role; each opened connection owns its protocol state.
pub trait Protocol: Sized {
    type Config;
    type Data;
    type Native: std::ops::DerefMut<Target = rustls::ConnectionCommon<Self::Data>> + 'static;

    #[doc(hidden)]
    const SEND_ON_OPEN: bool;

    #[doc(hidden)]
    fn open(config: &Self::Config, seal: Contract) -> Result<Self::Native, crate::Error>;
}

impl Protocol for Server {
    type Config = Arc<rustls::ServerConfig>;
    type Data = rustls::server::ServerConnectionData;
    type Native = rustls::ServerConnection;
    const SEND_ON_OPEN: bool = false;

    fn open(config: &Self::Config, _seal: Contract) -> Result<Self::Native, crate::Error> {
        let mut conn = rustls::ServerConnection::new(Arc::clone(config))?;
        conn.reject_early_data();
        Ok(conn)
    }
}

impl Protocol for Client {
    type Config = client::Plan;
    type Data = rustls::client::ClientConnectionData;
    type Native = rustls::ClientConnection;
    const SEND_ON_OPEN: bool = true;

    fn open(config: &Self::Config, _seal: Contract) -> Result<Self::Native, crate::Error> {
        Ok(rustls::ClientConnection::new_with_alpn(
            Arc::clone(&config.config),
            config.name.clone(),
            Block::try_clone(config.config.alpn_protocols.as_slice(), |protocol| {
                Ok(Box::<[u8]>::try_copy(protocol)?.into_vec())
            })?
            .into_vec(),
        )?)
    }
}
