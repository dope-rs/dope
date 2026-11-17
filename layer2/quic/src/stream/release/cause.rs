/// Why QUIC retired a send owner or send half.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    Acked,
    /// RESET_STREAM acknowledgement retired the send half; no owners are released again.
    ResetAcked,
    Stopped,
    Reset,
    Closed,
    PeerRetired,
}
