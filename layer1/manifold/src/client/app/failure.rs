/// Connection failures are owned entirely by the connector retry controller.
pub enum Controlled {}

/// Connection failures are also delivered to the application.
pub enum Observed {}

pub trait Mode {}

impl Mode for Controlled {}
impl Mode for Observed {}
