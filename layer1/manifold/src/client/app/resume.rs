use crate::client::app;

/// Statically selects complete or resumable receive continuation behavior.
pub trait Mode {
    type Outcome;
    type Retention;
}

pub struct Complete;
pub struct Retained;
pub struct Absent;
pub struct Stored;

impl Mode for Complete {
    type Outcome = app::ChunkOutcome;
    type Retention = Absent;
}

impl Mode for Retained {
    type Outcome = app::Resumed;
    type Retention = Stored;
}
