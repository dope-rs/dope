use std::sync::Arc;

use o3::buffer;
use rustls::{ServerConfig, quic};

use crate::{conn, errors, security::qkdf, stream};
use conn::{life, wire};
use life::{accept, initial::assembly, options, tls::servers};
use wire::{path::setup, tls};

pub(crate) struct Acceptor {
    ids: accept::Ids,
    input: options::Options,
}

impl Acceptor {
    pub(crate) fn new(ids: accept::Ids, opts: options::Options) -> Self {
        Self { ids, input: opts }
    }

    pub(crate) fn try_owned<const DOMAIN: u8, B, D>(
        self,
        config: Arc<ServerConfig>,
        keys: Option<qkdf::Pair>,
    ) -> Result<conn::Connection<DOMAIN, B, D>, errors::Connect>
    where
        B: stream::Recv,
        D: buffer::raw::Segment,
    {
        let reset = self
            .input
            .security
            .stateless_reset_secret
            .as_ref()
            .map(|secret| secret.token_for(&self.ids.local_cid));
        let mut params = Vec::new();
        self.input
            .transport
            .params()
            .bind_server(
                &self.ids.local_cid,
                &self.ids.tp_original_dcid,
                self.ids.retry_scid.as_ref(),
                reset.as_ref(),
            )
            .encode(&mut params)?;
        let state = quic::ServerConnection::new(config, quic::Version::V1, params)
            .map_err(errors::Connect::Setup)?;
        let keys = match keys {
            Some(keys) => keys,
            None => qkdf::Initial::try_new(&self.ids.initial_dcid)
                .and_then(qkdf::Pair::try_server)
                .map_err(|error| errors::Connect::Protocol(error.into()))?,
        };
        let hs = tls::Handshake::try_new(
            state.into(),
            keys.peer,
            keys.local,
            self.input.journals.crypto(),
            None,
        )
        .map_err(errors::Connect::TlsCrypto)?;
        let Self { ids, input } = self;
        assembly::Assembly {
            input,
            wire: hs,
            identities: setup::Identities {
                local_cid: ids.local_cid,
                original_dcid: ids.initial_dcid,
                peer_cid: ids.peer_cid,
                peer_first_scid: Some(ids.peer_cid),
            },
            tokens: setup::Tokens::Server(ids.issued_address_token),
            shard: None,
            valid: ids.peer_address_validated,
        }
        .try_finish(&mut None)
    }

    pub(crate) fn try_pooled<'pool, const DOMAIN: u8, B, D>(
        self,
        pool: &'pool servers::Pool<DOMAIN>,
        keys: Option<qkdf::Pair>,
    ) -> Result<(conn::Connection<DOMAIN, B, D>, life::tls::Permit<'pool>), errors::Connect>
    where
        B: stream::Recv,
        D: buffer::raw::Segment,
    {
        let permit = pool.reserve()?;
        let conn = self.try_owned(pool.authority().config(), keys)?;
        Ok((conn, permit))
    }
}
