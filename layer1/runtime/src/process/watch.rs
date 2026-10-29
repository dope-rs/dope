use o3::base::time::Positive;

/// Main-process notification performed after every required worker is ready.
pub trait Notify {
    type Error;
    type Heartbeat: Heartbeat<Self::Error>;

    fn ready(self) -> Result<Self::Heartbeat, Self::Error>;
}

/// Explicit steady-state watch owned by the process manager.
pub trait Heartbeat<E> {
    fn period(&self) -> Positive;
    fn pulse(&self) -> Result<(), E>;

    fn draining(&self) -> Result<(), E>;
}
