use crate::security::secrets;

#[derive(Default)]
pub struct Security {
    pub(crate) stateless_reset_secret: Option<secrets::Reset>,
    pub(crate) require_address_validation: bool,
    pub(crate) retry_token_secret: Option<secrets::Retry>,
}

impl Security {
    /// Configures the secret used to derive tokens for locally issued CIDs.
    ///
    /// `None` disables additional local CID issuance as well as stateless-reset
    /// generation. The initial CID remains usable. A nonempty local CID and a
    /// secret allow the runtime to supply additional CIDs within the peer's
    /// negotiated limit.
    ///
    /// Each endpoint must supply unused CIDs for its peer to respond on new
    /// paths. Without an unused peer CID, path validation waits for a
    /// NEW_CONNECTION_ID; it does not reuse another path's CID. Configure both
    /// endpoints when address changes are required. A secret alone does not
    /// guarantee migration: address policy, validation and route capacity still
    /// apply.
    pub fn reset(mut self, value: Option<[u8; 32]>) -> Self {
        self.stateless_reset_secret = value.map(secrets::Reset);
        self
    }

    pub fn retry(mut self, secret: [u8; 32]) -> Self {
        self.require_address_validation = true;
        self.retry_token_secret = Some(secrets::Retry(secret));
        self
    }

    pub fn validation(mut self, secret: [u8; 32]) -> Self {
        self.retry_token_secret = Some(secrets::Retry(secret));
        self
    }
}
