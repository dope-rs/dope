/// Delivers every connection-completion fact to the application.
pub enum Full {}

/// Delivers only the connected peer and connection-owned turn context.
pub enum Peer {}

pub trait Mode {}

impl Mode for Full {}
impl Mode for Peer {}
