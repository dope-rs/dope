/// Keeps retryable socket-open deferral inside the connector.
pub enum Controlled {}

/// Delivers retryable socket-open deferral to the application.
pub enum Observed {}

pub trait Mode {}

impl Mode for Controlled {}
impl Mode for Observed {}
