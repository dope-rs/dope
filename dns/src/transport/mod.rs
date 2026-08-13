pub mod datagram;
pub mod stream;

/// Start with datagrams and retry a truncated response over a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatagramThenStream;
