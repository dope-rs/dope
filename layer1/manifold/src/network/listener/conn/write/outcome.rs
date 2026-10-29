use crate::network::listener::conn::write;

/// Transport-owned outcome of one logical write transaction.
pub enum Outcome<R> {
    Committed(R),
    Deferred(write::Deferred),
    Closed,
}
