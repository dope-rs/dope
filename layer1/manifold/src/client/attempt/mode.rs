/// Classifies whether attempts are owned by one connector or a service pool.
pub trait Mode {}

/// Attempts owned directly by one connector.
pub struct Local;

impl Mode for Local {}

/// Attempts selected and retained by a service pool.
pub struct Service;

impl Mode for Service {}
